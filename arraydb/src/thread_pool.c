#include "thread_pool.h"

static thread_pool_t *pool = NULL;

void pool_init(int max_thread_num) {
    pool = (thread_pool_t *)malloc(sizeof(thread_pool_t));

    pthread_mutex_init(&(pool->queue_lock), NULL);
    pthread_mutex_init(&(pool->arg_lock), NULL);
    pthread_cond_init(&(pool->queue_ready), NULL);

    pool->max_thread_num = max_thread_num;
    pool->cur_queue_size = 0;
    pool->queue_head = NULL;
    pool->queue_tail = NULL;
    pool->shutdown = 0;
    pool->threads = (pthread_t *)malloc(max_thread_num * sizeof(pthread_t));

    int i;
    for (i = 0; i < max_thread_num; i++) 
	pthread_create(&(pool->threads[i]), NULL, thread_run, NULL);
}

int pool_destroy() {
    if (pool->shutdown)
	return -1;

    int i;
    job_t *tjob;

    pool->shutdown = 1;
    pthread_cond_broadcast(&(pool->queue_ready));
    for (i = 0; i < pool->max_thread_num; i++)
	pthread_join(pool->threads[i], NULL);
    free(pool->threads);
    pool->threads = NULL;

    pool->queue_tail = NULL;
    while(pool->queue_head) {
	tjob = pool->queue_head;
	pool->queue_head = tjob->next;
	free(tjob);
	tjob = NULL;
    }

    pthread_mutex_destroy(&(pool->queue_lock));
    pthread_mutex_destroy(&(pool->arg_lock));
    pthread_cond_destroy(&(pool->queue_ready));
    free(pool);
    pool = NULL;
    return 0;
}

int pool_add_job(void *(*process)(void *arg), void *arg) {
    job_t *newjob;

    newjob = (job_t *)malloc(sizeof(job_t));
    assert(newjob);
    newjob->process = process;
    newjob->arg = arg;
    newjob->next = NULL;

    pthread_mutex_lock(&(pool->queue_lock));
    // lock section
    if (pool->queue_tail) {
	pool->queue_tail->next = newjob;
	pool->queue_tail = newjob;
    }
    else {
	assert(pool->queue_head == NULL);
	pool->queue_head = pool->queue_tail = newjob;
    }
    pool->cur_queue_size++;
    // end lock section
    pthread_mutex_unlock(&(pool->queue_lock));
    pthread_cond_signal(&(pool->queue_ready));
    return 0;
}

void *thread_run(void *arg) {
    printf("starting thread\n");

    while (1) {
	pthread_mutex_lock(&(pool->queue_lock));

	// lock section
	while (!pool->cur_queue_size && !pool->shutdown) {
	    pthread_cond_wait(&(pool->queue_ready), &(pool->queue_lock));
	}

	if (pool->shutdown && pool->cur_queue_size == 0) {
	    pthread_mutex_unlock(&(pool->queue_lock));
	    printf("thread exit\n");
	    pthread_exit(NULL);
	}


	assert(pool->cur_queue_size);
	assert(pool->queue_head);

	pool->cur_queue_size--;
	job_t *job = pool->queue_head;

	if (pool->cur_queue_size == 0)
	    // job queue is already empty
	    pool->queue_head = pool->queue_tail = NULL;
	else
	    pool->queue_head = job->next;
	// end lock section
	pthread_mutex_unlock(&(pool->queue_lock));

	// thread process the job
	(*(job->process))(job->arg);
	free(job);
	job = NULL;
    }
    assert(0);
}

void pool_arg_lock() {
    pthread_mutex_lock(&(pool->arg_lock));
}

void pool_arg_unlock() {
    pthread_mutex_unlock(&(pool->arg_lock));
}
