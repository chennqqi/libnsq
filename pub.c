/*pub.c*/
/********************************************************************************
PUB <topic_name>\n
[ 4-byte size in bytes ][ N-byte binary data ]
<topic_name> - a valid string (optionally having #ephemeral suffix)

*********************************************************************************/

/*
TODO: handel signal 
MSG_NOSIGNAL SIT_PIPE 
EINTR
EAGAIN
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>

#include "pub.h"




struct publisher{
	int flags;
	int fd;	
};

PUBLISHER newNsqPublisher(const char *address, int port, int flags) {
	struct sockaddr_in serv_addr = {0,};
	int sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) {
		return NULL;
	}

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = inet_addr(caddress);
	serv_addr.sin_port = htons(atoi(port));

	if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) == -1) {
		return NULL;
	}
	int r = write(sock, "  V2", 4);
	if (r != 4) {
		close(sock);
		return NULL;
	}
	struct publisher* pPub = (struct publisher*)calloc(sizeof(struct publisher), 1);
	pPub->fd = sock;
	pPub->flags = flags;
	if (flags & NSQ_PUB_ASYNC) {
		//TODO: async support
	}
	return (void*)pPub;
}

void destroyNsqPublisher(PUBLISHER p) {
	struct publisher* pPub = (struct publisher*)p;
	if (pPub != NULL) {
		close(pPub->sock);
		free(pPub);
	}
}

static int tcp_send(int sock, const void* p, const size_t len) {
	int r = 0;
	for (;;) {
		r = write(pPub->sock, p, len);
		if (r == len) {
			break;
		} else if (r == -1 && errno=EINTR) {			
			continue
		} else {
			return errno;
		}
	}
	return r;
}

static int tcp_send(int sock, const void* p, const size_t len) {
	int r = 0;
	for (;;) {
		r = write(sock, p, len);
		if (r == len) {
			break;
		} else if (r == -1 && errno=EINTR) {			
			continue
		} else {
			return errno;
		}
	}
	return r;
}

static int tcp_send_until(int sock, const void* p, const size_t len) {
	int r = 0;
	int offset = 0;
	for (;;) {
		r = write(sock, (char*)p+offset, len-offset);
		if (r > 0) {
			offset += r;
			if (r == len) {
				return offset;
			}
		} else if (r < 0) {
			int err = errno;
			if (err==EINTR) {
				continue
			} else {
				return err;
			}
		} else {
			return -errno;
		}
	}
	return offset;
}

static int tcp_read(int sock, void* p, const size_t len) {
	int r = 0;
	int offset = 0;
	for (;;) {
		r = read(sock, p, len);
		if (r > 0) {
			return r;
		} else if (r < 0) {
			int err = errno;
			if (err==EINTR) {
				continue
			} else {
				return err;
			}
		} else {
			return -errno;
		}
	}
}

int nsq_publish(PUBLISHER p, const char* pszTopic, const void* p, const size_t len) {
	struct publisher* pPub = (struct publisher*)p;
	char head[512] = {0};
	int r = snprintf(head, sizeof(head), "PUB %s\n", pszTopic);
	r = tcp_send_until(pPub->sock, head, r);
	if (r < 0) {
		return r;
	}

	int txlen = htonl(len);
	r = tcp_send_until(pPub->sock, &txLen, 4);
	if (r < 0) {
		return r;
	}
	r = tcp_send_until(pPsub->sock, p, len);
	if (r < 0) {
		return r;
	}
	
	while (1) {
		char msg[64] = {0};
		int l = tcp_read_until(pPub->sock, msg, 2);
		if (l == 2) {
			if (strcmp(msg, "OK") == 0) {
				return len;//publish OK
			} else if (strcmp(msg, "E_") == 0) {
				/*
					E_INVALID
					E_BAD_TOPIC
					E_BAD_BODY
					E_BAD_MESSAGE
					E_MPUB_FAILED
				*/
				l = tcp_read(sock, msg+2, sizeof(msg)-2);
				if (l < 0) {
					return l;
				}
				printf("publish error: %s\n", msg);
				return -1;
			}			
			break;
		} else {
			return l;
		}
	}
	return 0;
}

int nsq_multi_publish(PUBLISHER p, const char* pszTopic, const nsq_rawmsg* pM, const size_t count) {
/*
MPUB <topic_name>\n
[ 4-byte body size ]
[ 4-byte num messages ]
[ 4-byte message #1 size ][ N-byte binary data ]
      ... (repeated <num_messages> times)
<topic_name> - a valid string (optionally having #ephemeral suffix)
*/	
	struct publisher* pPub = (struct publisher*)p;
	int r = fprintf(pPub->sock, "MPUB %s\n", pszTopic);
	int i=0;
	for (i=0; i<count; i++) {
		int txlen = htonl(len);
		r = tcp_send(pPub->sock, &txLen, 4);
		if (r < 0) {
			return i-1;
		}
		r = tcp_send(pPsub->sock, pM[i]->p, pM[i]->len);
		if (r < 0) {
			return i-1;
		}
	}
	
	while (1) {
		char msg[64] = {0};
		int l = tcp_read_until(pPub->sock, msg, 2);
		if (l == 2) {
			if (strcmp(msg, "OK") == 0) {
				return len;//publish OK
			} else if (strcmp(msg, "E_") == 0) {
				/*
				 E_INVALID
				 E_BAD_TOPIC
				 E_BAD_BODY
				 E_BAD_MESSAGE
				 E_MPUB_FAILED
				*/
				l = tcp_read(sock, msg+2, sizeof(msg)-2);
				if (l < 0) {
					return l;
				}
				printf("publish error: %s\n", msg);
				return -1;
			}			
			break;
		} else {
			return l;
		}
	}
	return 0;
}

int nsq_async_publish(PUBLISHER pub, const char* pszTopic, const void* p, const size_t len) {
	return -1;
}

int nsq_async_multi_publish(PUBLISHER pub, const char* pszTopic, const nsq_rawmsg* p, const size_t count) {
	return -1;
}

