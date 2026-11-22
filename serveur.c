#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "serveur.h"

/* ------------------------------------------------------------------ */
/* Structures                                                          */
/* ------------------------------------------------------------------ */
typedef struct {
    int   in_use;
    pid_t owner;
    long  granted_at;      /* secondes (horloge monotone) */
} License;

typedef struct {
    int   active;
    pid_t pid;
    int   last_license;    /* historique minimal : dernière licence utilisée, -1 sinon */
} Client;

typedef struct {
    pid_t pid;
    int   preferred;       /* licence que ce client préférerait (son historique) */
} Waiter;

static License lic[NB_LICENSES];
static Client  clients[MAX_CLIENTS];
static Waiter  queue[MAX_CLIENTS];
static int     q_len = 0;

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;  /* protège TOUT l'état ci-dessus */
static sem_t           clients_sem;                       /* limite de M clients          */
static volatile sig_atomic_t running = 1;
static long t0;

/* ------------------------------------------------------------------ */
/* Utilitaires                                                         */
/* ------------------------------------------------------------------ */
static long now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}

static void slog(const char *fmt, ...) {
    va_list ap;
    printf("[%3lds] ", now_s() - t0);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

/* Envoie une réponse au client. Retourne -1 si le client est injoignable
 * (file absente ou pleine) -> on le considère comme déconnecté.
 * Non bloquant : sûr à appeler en tenant le mutex. */
static int send_response(pid_t pid, RspType type, int id) {
    char name[64];
    snprintf(name, sizeof name, CLIENT_QUEUE_FMT, (int)pid);
    mqd_t q = mq_open(name, O_WRONLY | O_NONBLOCK);
    if (q == (mqd_t)-1) return -1;
    Response r = { type, id };
    int rc = mq_send(q, (const char *)&r, sizeof r, 0);
    mq_close(q);
    return rc;
}

static Client *find_client(pid_t pid) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].active && clients[i].pid == pid) return &clients[i];
    return NULL;
}

static int is_waiting(pid_t pid) {
    for (int i = 0; i < q_len; i++)
        if (queue[i].pid == pid) return 1;
    return 0;
}

static int owns_license(pid_t pid) {
    for (int k = 0; k < NB_LICENSES; k++)
        if (lic[k].in_use && lic[k].owner == pid) return 1;
    return 0;
}

static void queue_remove_at(int idx) {
    for (int i = idx; i < q_len - 1; i++) queue[i] = queue[i + 1];
    q_len--;
}

static void queue_remove_pid(pid_t pid) {
    for (int i = 0; i < q_len; i++)
        if (queue[i].pid == pid) { queue_remove_at(i); return; }
}

static void assign(int k, Client *c) {
    lic[k].in_use     = 1;
    lic[k].owner      = c->pid;
    lic[k].granted_at = now_s();
    c->last_license   = k;
    slog("licence %d -> client %d", k, (int)c->pid);
}

/* ------------------------------------------------------------------ */
/* Cœur : libération + redistribution (mutex DÉJÀ pris)                */
/* ------------------------------------------------------------------ */
static void release_license(int k);
static void remove_client_locked(Client *c);

/* Donne la licence libre k au bon client en attente :
 * priorité à celui qui l'avait déjà utilisée, sinon tête de file. */
static void dispatch(int k) {
    while (!lic[k].in_use && q_len > 0) {
        int idx = 0;
        for (int i = 0; i < q_len; i++)
            if (queue[i].preferred == k) { idx = i; break; }

        Waiter w = queue[idx];
        queue_remove_at(idx);

        Client *c = find_client(w.pid);
        if (!c) continue;

        if (send_response(w.pid, RSP_GRANT, k) == 0)
            assign(k, c);
        else {
            slog("client %d injoignable pendant l'attente -> retire", (int)w.pid);
            remove_client_locked(c);
        }
    }
}

/* Point d'entrée UNIQUE pour libérer : release volontaire, expiration, déconnexion. */
static void release_license(int k) {
    slog("licence %d libre", k);
    lic[k].in_use = 0;
    lic[k].owner  = 0;
    dispatch(k);
}

static void remove_client_locked(Client *c) {
    pid_t pid = c->pid;
    queue_remove_pid(pid);
    c->active = 0;
    sem_post(&clients_sem);
    slog("client %d deconnecte", (int)pid);
    for (int k = 0; k < NB_LICENSES; k++)
        if (lic[k].in_use && lic[k].owner == pid)
            release_license(k);
}

/* ------------------------------------------------------------------ */
/* Traitement des requêtes                                             */
/* ------------------------------------------------------------------ */
static void handle_connect(pid_t pid) {
    pthread_mutex_lock(&mtx);
    if (find_client(pid)) {
        send_response(pid, RSP_OK, -1);
    } else if (sem_trywait(&clients_sem) != 0) {
        slog("client %d refuse (serveur plein)", (int)pid);
        send_response(pid, RSP_FULL, -1);
    } else {
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) {
                clients[i] = (Client){ 1, pid, -1 };
                break;
            }
        }
        slog("client %d connecte", (int)pid);
        send_response(pid, RSP_OK, -1);
    }
    pthread_mutex_unlock(&mtx);
}

/* wanted : numéro de licence souhaité, ou -1 (pas de préférence explicite) */
static void handle_request(pid_t pid, int wanted) {
    pthread_mutex_lock(&mtx);
    Client *c = find_client(pid);

    if (!c || owns_license(pid) || is_waiting(pid)
        || wanted < -1 || wanted >= NB_LICENSES) {
        send_response(pid, RSP_ERROR, -1);
    } else {
        /* préférence : celle demandée, sinon la dernière utilisée */
        int pref = (wanted >= 0) ? wanted : c->last_license;

        int k = -1;
        if (pref >= 0 && !lic[pref].in_use)
            k = pref;                       /* la licence voulue est libre */
        else
            for (int i = 0; i < NB_LICENSES; i++)   /* sinon, une autre libre */
                if (!lic[i].in_use) { k = i; break; }

        if (wanted >= 0 && k >= 0 && k != wanted)
            slog("licence %d demandee par %d indisponible -> propose %d",
                 wanted, (int)pid, k);

        if (k >= 0) {
            if (send_response(pid, RSP_GRANT, k) == 0)
                assign(k, c);
            else
                remove_client_locked(c);
        } else {
            queue[q_len++] = (Waiter){ pid, pref };
            slog("client %d en attente (file: %d)", (int)pid, q_len);
            send_response(pid, RSP_WAIT, -1);
        }
    }
    pthread_mutex_unlock(&mtx);
}

static void handle_release(pid_t pid, int k) {
    pthread_mutex_lock(&mtx);
    if (k >= 0 && k < NB_LICENSES && lic[k].in_use && lic[k].owner == pid)
        release_license(k);
    pthread_mutex_unlock(&mtx);
}

static void handle_disconnect(pid_t pid) {
    pthread_mutex_lock(&mtx);
    Client *c = find_client(pid);
    if (c) remove_client_locked(c);
    pthread_mutex_unlock(&mtx);
}

/* ------------------------------------------------------------------ */
/* Thread de surveillance : récupère les licences expirées             */
/* ------------------------------------------------------------------ */
static void *monitor_thread(void *arg) {
    (void)arg;
    while (running) {
        sleep(1);
        pthread_mutex_lock(&mtx);
        long now = now_s();
        for (int k = 0; k < NB_LICENSES; k++) {
            if (lic[k].in_use && now - lic[k].granted_at >= LICENSE_DUREE) {
                slog("licence %d expiree (client %d)", k, (int)lic[k].owner);
                send_response(lic[k].owner, RSP_EXPIRED, k);  /* best effort */
                release_license(k);
            }
        }
        pthread_mutex_unlock(&mtx);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */
static void on_signal(int sig) { (void)sig; running = 0; }

int main(void) {
    t0 = now_s();

    struct mq_attr attr = { .mq_maxmsg = 10, .mq_msgsize = sizeof(Request) };
    mq_unlink(SERVER_QUEUE);
    mqd_t qs = mq_open(SERVER_QUEUE, O_CREAT | O_RDONLY, 0666, &attr);
    if (qs == (mqd_t)-1) { perror("mq_open"); return 1; }

    sem_init(&clients_sem, 0, MAX_CLIENTS);

    /* SIGINT/SIGTERM doivent être reçus par le thread principal uniquement */
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    pthread_t mon;
    pthread_create(&mon, NULL, monitor_thread, NULL);

    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    struct sigaction sa = { .sa_handler = on_signal };   /* pas de SA_RESTART : mq_receive -> EINTR */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    slog("serveur demarre : %d licences, %d clients max, duree licence max %ds",
         NB_LICENSES, MAX_CLIENTS, LICENSE_DUREE);

    while (running) {
        Request req;
        ssize_t n = mq_receive(qs, (char *)&req, sizeof req, NULL);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("mq_receive");
            break;
        }
        switch (req.type) {
            case MSG_CONNECT:    handle_connect(req.pid);                  break;
            case MSG_REQUEST:    handle_request(req.pid, req.license_id);  break;
            case MSG_RELEASE:    handle_release(req.pid, req.license_id);  break;
            case MSG_DISCONNECT: handle_disconnect(req.pid);               break;
        }
    }

    slog("arret du serveur");
    running = 0;
    pthread_join(mon, NULL);
    mq_close(qs);
    mq_unlink(SERVER_QUEUE);
    sem_destroy(&clients_sem);
    return 0;
}
