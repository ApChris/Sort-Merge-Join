#ifndef JOBSCHEDULER_H
#define JOBSCHEDULER_H

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>

#define THREADS 4

typedef struct job
{
	void (*job_to_do)(void *);
	void * job_arguments;
	struct job * next_job;
} job;


typedef struct job_scheduler
{
	uint64_t total_threads;
	uint64_t jobs_left;
	uint64_t finished; // to exit and destroy threads
	job * job_queue;

	pthread_t * threads;
	pthread_mutex_t  queue_thread_access; // access to the same queue from multiple threads
	pthread_cond_t barrier_cond_var; // scheduler is waiting for threads
	sem_t queue_job_sem; // checking if there are jobs available in the queue

} job_scheduler;

job_scheduler * Scheduler_Init(int total_threads);
job * Assign_Job(job_scheduler * scheduler);
void Complete_Job(job_scheduler * scheduler);
void *Thread_Routine(void *thread_pool);

#endif