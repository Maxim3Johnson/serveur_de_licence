
CC     = gcc
CFLAGS = -Wall -Wextra -pthread -g
LDLIBS = -lrt
 
all: serveur client
 
server: serveur.c serveur.h
	$(CC) $(CFLAGS) -o $@ serveur.c $(LDLIBS)
 
client: client.c serveur.h
	$(CC) $(CFLAGS) -o $@ client.c $(LDLIBS)
 
clean:
	rm -f server client
 
