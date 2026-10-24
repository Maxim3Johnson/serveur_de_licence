CC=gcc
CFLAGS=-Wall -Wextra -O2 -pthread
all: serveur client
server: serveur.c licence.h
	$(CC) $(CFLAGS) -o $@ $<
client: client.c licence.h
	$(CC) $(CFLAGS) -o $@ $<
clean:
	rm -f server client /tmp/lic_*
