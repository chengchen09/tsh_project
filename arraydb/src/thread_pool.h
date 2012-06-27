#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

typedef struct _job {
	void *(*process)(void *arg);
	void *arg;
	struct _job *next;
}job_t;

/* thread poll */
typedef struct {
	pthread_mutex_t queue_lock;
	pthread_mutex_t arg_lock;
	pthread_cond_t queue_ready;

	job_t *queue_head;
	job_t *queue_tail;

	int shutdown;
	pthread_t *threads;
	int max_thread_num;
	int cur_queue_size;
}thread_pool_t;

int pool_add_job(void *(*process)(void *arg), void *arg);
void *thread_run(void *arg);
void pool_init(int max_thread_num);
int pool_destroy();
void pool_arg_lock();
void pool_arg_unlock();

#endif
