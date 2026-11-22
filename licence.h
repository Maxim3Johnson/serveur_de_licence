#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "serveur.h"

/* Usage : ./client
 * Le client reste connecte et lit des lignes "licence duree" au clavier :
 *     2 5    -> demande la licence 2 pendant 5 s
 *    -1 3    -> n'importe quelle licence pendant 3 s
 * Ctrl-C (ou Ctrl-D) : deconnexion propre, la licence eventuelle est rendue. */

static mqd_t sq;   /* file des requêtes du serveur */
static mqd_t rq;   /* ma file de réponses          */
static char  rq_name[64];
static int   me;

static volatile sig_atomic_t interrupted = 0;

/* Le handler note seulement l'événement ; le travail est fait dans main. */
static void on_signal(int sig) { (void)sig; interrupted = 1; }

static void send_req(ReqType type, int id) {
    Request r = { type, me, id };
    if (mq_send(sq, (const char *)&r, sizeof r, 0) < 0) perror("mq_send");
}

/* timeout_s < 0 : bloquant ; 0 : ne bloque pas. Retourne 0 si réponse reçue,
 * -1 si timeout, erreur ou interruption par un signal (EINTR). */
static int recv_rsp(Response *out, int timeout_s) {
    if (timeout_s < 0)
        return mq_receive(rq, (char *)out, sizeof *out, NULL) < 0 ? -1 : 0;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_s;
    return mq_timedreceive(rq, (char *)out, sizeof *out, NULL, &ts) < 0 ? -1 : 0;
}

static void cleanup(void) {
    mq_close(rq);
    mq_unlink(rq_name);
    mq_close(sq);
}

/* Un cycle : demander -> attendre -> utiliser -> rendre.
 * *k contient la licence détenue (-1 si aucune), pour que main puisse
 * la signaler si un signal arrive en cours de route.
 * Retourne 0 si on peut continuer, -1 si on doit quitter. */
static int run_cycle(int wanted, int hold, int *k) {
    Response r;

    /* Purge : un RSP_EXPIRED tardif d'un cycle précédent ne doit pas
     * être pris pour la réponse à cette nouvelle demande. */
    while (recv_rsp(&r, 0) == 0)
        ;

    if (wanted >= 0) printf("[%d] demande la licence %d\n", me, wanted);
    send_req(MSG_REQUEST, wanted);

    if (recv_rsp(&r, 5) < 0) {
        if (!interrupted) printf("[%d] pas de reponse du serveur\n", me);
        return -1;
    }
    if (r.type == RSP_WAIT) {
        printf("[%d] en attente d'une licence...\n", me);
        if (recv_rsp(&r, -1) < 0) {
            if (interrupted) printf("[%d] interrompu pendant l'attente\n", me);
            return -1;
        }
    }
    if (r.type != RSP_GRANT) {
        printf("[%d] demande refusee (numero invalide ?)\n", me);
        return 0;                                   /* on peut reessayer */
    }

    *k = r.license_id;
    if (wanted >= 0 && *k != wanted)
        printf("[%d] licence %d indisponible, on me propose la %d\n", me, wanted, *k);
    printf("[%d] licence %d obtenue, utilisation %ds\n", me, *k, hold);

    /* utilisation : on attend 'hold' s, un message EXPIRED, ou un signal */
    if (recv_rsp(&r, hold) == 0 && r.type == RSP_EXPIRED) {
        printf("[%d] licence %d retiree par le serveur (expiree)\n", me, *k);
        *k = -1;                                    /* déjà reprise */
    } else if (interrupted) {
        return -1;                                  /* main s'occupe de la sortie */
    } else {
        send_req(MSG_RELEASE, *k);                  /* on reste connecté */
        printf("[%d] licence %d rendue\n", me, *k);
        *k = -1;
    }
    return 0;
}

int main(void) {
    me = getpid();

    snprintf(rq_name, sizeof rq_name, CLIENT_QUEUE_FMT, me);
    struct mq_attr attr = { .mq_maxmsg = 10, .mq_msgsize = sizeof(Response) };
    mq_unlink(rq_name);
    rq = mq_open(rq_name, O_CREAT | O_RDONLY, 0600, &attr);
    sq = mq_open(SERVER_QUEUE, O_WRONLY);
    if (rq == (mqd_t)-1 || sq == (mqd_t)-1) { perror("mq_open (serveur lance ?)"); return 1; }

    /* Pas de SA_RESTART : mq_receive ET fgets sont interrompus par Ctrl-C */
    struct sigaction sa = { .sa_handler = on_signal };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    Response r;
    int k = -1;          /* licence détenue, -1 si aucune */
    int status = 0;

    /* connexion (une seule fois) */
    send_req(MSG_CONNECT, -1);
    if (recv_rsp(&r, 5) < 0 || r.type != RSP_OK) {
        if (!interrupted) printf("[%d] connexion refusee (serveur plein ?)\n", me);
        status = 1;
        goto end;
    }
    printf("[%d] connecte. Saisir \"licence duree\" (licence -1 = peu importe), Ctrl-C pour quitter.\n", me);

    /* boucle principale : on ne sort que sur Ctrl-C / Ctrl-D / erreur grave */
    while (!interrupted) {
        char line[64];
        int wanted, hold;

        printf("[%d] licence duree > ", me);
        fflush(stdout);
        if (!fgets(line, sizeof line, stdin))       
            break;
        if (sscanf(line, "%d %d", &wanted, &hold) != 2 || hold <= 0
            || wanted < -1 || wanted >= NB_LICENSES) {
            printf("format : <licence 0..%d ou -1> <duree en s > 0>\n", NB_LICENSES - 1);
            continue;
        }
        if (run_cycle(wanted, hold, &k) < 0) break;
    }
    printf("\n");

end:
    /* sortie propre : MSG_DISCONNECT libere aussi les licences du client
     * et le retire de la file (sans effet si le serveur ne le connait pas). */
    if (k >= 0)
        printf("[%d] deconnexion : la licence %d est rendue avec\n", me, k);
    else
        printf("[%d] deconnexion\n", me);
    send_req(MSG_DISCONNECT, -1);
    cleanup();
    return status;
}
