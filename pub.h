#ifndef __pub_h
#define __pub_h

#ifdef __cplusplus
extern "C"{
#endif

enum {
	NSQ_PUB_ASYNC = 0x01,
}

typedef void* PUBLISHER;

struct nsq_rawmsg {
	const void* p;
	const size_t len;
}

PUBLISHER newNsqPublisher(const char *address, int port, int flags);
void destroyNsqPublisher(PUBLISHER pub);

int nsq_publish(PUBLISHER pub, const char* pszTopic, const void* p, const size_t len);
int nsq_multi_publish(PUBLISHER pub, const char* pszTopic, const nsq_rawmsg* p, const size_t count);

int nsq_async_publish(PUBLISHER pub, const char* pszTopic, const void* p, const size_t len);
int nsq_async_multi_publish(PUBLISHER pub, const char* pszTopic, const nsq_rawmsg* p, const size_t count);



#ifdef __cplusplus
}
#endif

#endif /*__pub_h*/
