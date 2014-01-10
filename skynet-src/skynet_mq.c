#include "skynet.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "skynet_multicast.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#define DEFAULT_QUEUE_SIZE 64
#define MAX_GLOBAL_MQ 0x10000

// 0 means mq is not in global mq.
// 1 means mq is in global mq , or the message is dispatching.
// 2 means message is dispatching with locked session set.
// 3 means mq is not in global mq, and locked session has been set.

#define MQ_IN_GLOBAL 1
#define MQ_DISPATCHING 2
#define MQ_LOCKED 3

struct message_queue {
	uint32_t handle;
	int cap;
	int head;
	int tail;
	int lock;
	int release; //xxx 用于标记该私有消息队列是否需要被清理
	int lock_session;
	int in_global;//xxx 该消息队列是否在全局消息队列中 0--未在 1--在
	struct skynet_message *queue;//xxx 存放消息对象的一维数组
};

struct global_queue {
	uint32_t head;//xxx queue[0]指向第一个消息队列
	uint32_t tail;//xxx queue[x]指向最后一个消息队列
	struct message_queue ** queue;
	bool * flag;//xxx 用于标记对应的位置是否挂载了消息队列

};

static struct global_queue *Q = NULL;

#define LOCK(q) while (__sync_lock_test_and_set(&(q)->lock,1)) {}
#define UNLOCK(q) __sync_lock_release(&(q)->lock);

#define GP(p) ((p) % MAX_GLOBAL_MQ)

static void 
skynet_globalmq_push(struct message_queue * queue) {
	struct global_queue *q= Q;

	uint32_t tail = GP(__sync_fetch_and_add(&q->tail,1));
	q->queue[tail] = queue;
	__sync_synchronize();
	q->flag[tail] = true;
}

struct message_queue * 
skynet_globalmq_pop() {
	struct global_queue *q = Q;
	uint32_t head =  q->head;
	uint32_t head_ptr = GP(head);
	if (head_ptr == GP(q->tail)) {
		return NULL;
	}

	if(!q->flag[head_ptr]) {
		return NULL;
	}

	__sync_synchronize();

	struct message_queue * mq = q->queue[head_ptr];
	if (!__sync_bool_compare_and_swap(&q->head, head, head+1)) {
		return NULL;
	}
	q->flag[head_ptr] = false;

	return mq;
}

/**
@brief   为服务创建私有消息队列
@param   handle 服务(ctx)的唯一标识    
@return  私有的消息队列
*/
struct message_queue * 
skynet_mq_create(uint32_t handle) {
	struct message_queue *q = malloc(sizeof(*q));
	q->handle = handle;
	q->cap = DEFAULT_QUEUE_SIZE;
	q->head = 0;
	q->tail = 0;
	q->lock = 0;
	q->in_global = MQ_IN_GLOBAL;//xxx 创建好就加入全局消息队列中
	q->release = 0; //xxx 标识消息队列是否需要被销毁
	q->lock_session = 0;
	q->queue = malloc(sizeof(struct skynet_message) * q->cap);

	return q;
}

static void 
_release(struct message_queue *q) {
	free(q->queue);
	free(q);
}

uint32_t 
skynet_mq_handle(struct message_queue *q) {
	return q->handle;
}


int
skynet_mq_pop(struct message_queue *q, struct skynet_message *message) {
	int ret = 1;
	LOCK(q)

	if (q->head != q->tail) {
		*message = q->queue[q->head];
		ret = 0;
		if ( ++ q->head >= q->cap) {
			q->head = 0;
		}
	}

	if (ret) {
		q->in_global = 0;
	}
	
	UNLOCK(q)

	return ret;
}

static void
expand_queue(struct message_queue *q) {
	struct skynet_message *new_queue = malloc(sizeof(struct skynet_message) * q->cap * 2);
	int i;
	for (i=0;i<q->cap;i++) {
		new_queue[i] = q->queue[(q->head + i) % q->cap];
	}
	q->head = 0;
	q->tail = q->cap;
	q->cap *= 2;
	
	free(q->queue);
	q->queue = new_queue;
}

static void
_unlock(struct message_queue *q) {
	// this api use in push a unlock message, so the in_global flags must not be 0 , 
	// but the q is not exist in global queue.
	if (q->in_global == MQ_LOCKED) {
		skynet_globalmq_push(q);
		q->in_global = MQ_IN_GLOBAL;
	} else {
		assert(q->in_global == MQ_DISPATCHING);
	}
	q->lock_session = 0;
}

/**
@breif	将消息加入服务私有消息队列的头部
*/
static void 
_pushhead(struct message_queue *q, struct skynet_message *message) {
	int head = q->head - 1;
	if (head < 0) {
		head = q->cap - 1;
	}
	if (head == q->tail) {
		expand_queue(q);
		--q->tail;
		head = q->cap - 1;
	}

	q->queue[head] = *message;
	q->head = head;

	_unlock(q);
}

/**
@breif	将一个消息放入到服务的私有消息队列
@param	q 私有消息队列
@param	message	待处理的消息
@return void
*/
void 
skynet_mq_push(struct message_queue *q, struct skynet_message *message) {
	assert(message);
	LOCK(q)
	
	if (q->lock_session !=0 && message->session == q->lock_session) {
		_pushhead(q,message);//xxx 加入队列头部
	} else {
		q->queue[q->tail] = *message;
		if (++ q->tail >= q->cap) {
			q->tail = 0;
		}

		if (q->head == q->tail) {
			expand_queue(q);
		}

		if (q->lock_session == 0) {
			if (q->in_global == 0) {//xxx 若该消息队列未在全局消息队列，则加入
				q->in_global = MQ_IN_GLOBAL;
				skynet_globalmq_push(q);
			}
		}
	}
	
	UNLOCK(q)
}

void
skynet_mq_lock(struct message_queue *q, int session) {
	LOCK(q)
	assert(q->lock_session == 0);
	assert(q->in_global == MQ_IN_GLOBAL);
	q->in_global = MQ_DISPATCHING;
	q->lock_session = session;
	UNLOCK(q)
}

void
skynet_mq_unlock(struct message_queue *q) {
	LOCK(q)
	_unlock(q);
	UNLOCK(q)
}

void 
skynet_mq_init() {
	struct global_queue *q = malloc(sizeof(*q));
	memset(q,0,sizeof(*q));
	q->queue = malloc(MAX_GLOBAL_MQ * sizeof(struct message_queue *));//xxx queue是一个指针，指向一块连续的大内存，这块连续大内存包含若干(struct message_queue *)指针
	q->flag = malloc(MAX_GLOBAL_MQ * sizeof(bool));//xxx 对应于每一个message_queue对象
	memset(q->flag, 0, sizeof(bool) * MAX_GLOBAL_MQ);//xxx 将整块内存置为0
	Q=q;//xxx 全局的消息队列
}

void 
skynet_mq_force_push(struct message_queue * queue) {
	assert(queue->in_global);
	skynet_globalmq_push(queue);
}

void 
skynet_mq_pushglobal(struct message_queue *queue) {
	LOCK(queue)
	assert(queue->in_global);
	if (queue->in_global == MQ_DISPATCHING) {
		// lock message queue just now.
		queue->in_global = MQ_LOCKED;
	}
	if (queue->lock_session == 0) {
		skynet_globalmq_push(queue);
		queue->in_global = MQ_IN_GLOBAL;
	}
	UNLOCK(queue)
}

void 
skynet_mq_mark_release(struct message_queue *q) {
	LOCK(q)
	assert(q->release == 0);
	q->release = 1;
	if (q->in_global != MQ_IN_GLOBAL) {
		skynet_globalmq_push(q);
	}
	UNLOCK(q)
}

static int
_drop_queue(struct message_queue *q) {
	// todo: send message back to message source
	struct skynet_message msg;
	int s = 0;
	while(!skynet_mq_pop(q, &msg)) {
		++s;
		int type = msg.sz >> HANDLE_REMOTE_SHIFT;
		if (type == PTYPE_MULTICAST) {
			assert((msg.sz & HANDLE_MASK) == 0);
			skynet_multicast_dispatch((struct skynet_multicast_message *)msg.data, NULL, NULL);
		} else {
			free(msg.data);
		}
	}
	_release(q);
	return s;
}

int 
skynet_mq_release(struct message_queue *q) {
	int ret = 0;
	LOCK(q)
	
	if (q->release) {
		UNLOCK(q)
		ret = _drop_queue(q);
	} else {
		skynet_mq_force_push(q);
		UNLOCK(q)
	}
	
	return ret;
}
