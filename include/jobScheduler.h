#ifndef JOBSCHEDULER_H
#define JOBSCHEDULER_H

#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <stdint.h>
#include "metadata.h"
#include "work.h"
#include "statistics.h"
#include "histogram.h"


#define THREADS 2

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
	job * jobs_queue_head;
	job * jobs_queue_tail;

	pthread_t * threads;
	pthread_mutex_t  queue_thread_access; // access to the same queue from multiple threads
	pthread_cond_t barrier_cond_var; // scheduler is waiting for threads
	sem_t queue_job_sem; // checking if there are jobs available in the queue

} job_scheduler;


typedef struct job_query
{
	metadata * md;
	work_line * wl_ptr;
	uint64_t query;
	statistics * stats;
	char c;
	job_scheduler * scheduler;
} job_query;


typedef struct job_hist
{
	relation * rel;
	histogram * hist;
	uint64_t sel_byte;
	uint64_t start;
	uint64_t end;
} job_hist;

// Initializes the job scheduler, creating THREADS total threads and every other struct member
job_scheduler * Init_JobScheduler(uint64_t num_threads);

// Pops a job from the queue, locking the mutex for accessing into the queue and unlocks when finishes
// used by the Thread_Routine
job * Next_Job(job_scheduler * scheduler);

// After popping a job from the queue, decreases the jobs_left of the scheduler
// if every job has been finished, signals the barrier
void Complete_Job(job_scheduler * scheduler);

// Makes the thread to wait until:
// 1) all jobs are successfuly executed and needs to exit or
// 2) assigns a job to thread to execute
void *Thread_Routine(void *thread_pool);

// Blocks at the barrier_cond_var until all threads have finished the jobs
void Barrier(job_scheduler * scheduler);

// Pushes a job to the queue, locking the mutex for accessing into the queue and unlocks when finishes
void Assign_Job(job_scheduler * scheduler, void * function, void * arguments);

// Destroys all threads and frees memory
void Destroy_JobScheduler(job_scheduler * scheduler);

void JobQuery(void *job_arguments);

void JobHist(void * job_arguments);
#endif
