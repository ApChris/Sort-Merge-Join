#include <stdio.h>
#include <stdlib.h>
#include "../include/jobScheduler.h"

job_scheduler * Scheduler_Init(int total_threads){

	job_scheduler * scheduler;
	if ((scheduler = (job_scheduler *)malloc(sizeof(job_scheduler)) == NULL))
	{
		perror("job_scheduler malloc error");
		exit(-1);
	}

	scheduler->total_threads = THREADS;
	scheduler->jobs_left = scheduler->total_threads;
	scheduler->finished = 0;
	// size of the queue is going to be THREADS+1
	scheduler->job_queue = malloc(sizeof(job) * (scheduler->total_threads + 1));

	scheduler->threads = malloc(sizeof(pthread_t) * scheduler->total_threads);
	pthread_mutex_init((&scheduler->queue_thread_access), NULL);
	pthread_cond_init((&scheduler->barrier_cond_var), NULL);

	// semaphore is shared between threads of a process (0) and starting value = 0
	sem_init((&scheduler->queue_job_sem), 0, 0);

	for (int i = 0; i < scheduler->total_threads; i++)
	{
		if(pthread_create(&(scheduler->threads[i]), NULL, Thread_Routine, scheduler) != 0){
			perror("pthread_create error");
			exit(-1);
		}
		scheduler->jobs_left--;
	}

	return scheduler;
}


job * Assign_Job(job_scheduler * scheduler){
	pthread_mutex_lock(&(scheduler->queue_thread_access));

	job * temp_job = scheduler->job_queue;
	scheduler->job_queue = scheduler->job_queue->next_job;

	pthread_mutex_unlock(&(scheduler->queue_thread_access));

	return temp_job;
}


void Complete_Job(job_scheduler * scheduler){
	pthread_mutex_lock(&(scheduler->queue_thread_access));

	scheduler->jobs_left--;
	if (scheduler->jobs_left == 0)
	{
		// wake the barrier condition
		pthread_cond_signal(&(scheduler->barrier_cond_var));
	}

	pthread_mutex_unlock(&(scheduler->queue_thread_access));	
}

void *Thread_Routine(void *thread_pool){
	job_scheduler * scheduler = (job_scheduler *) thread_pool;
	job * temp_job;


	while(1){
		// wait using semaphore for a job to be available at the queue
		sem_wait(&(scheduler->queue_job_sem));

		if (scheduler->finished == 1)
		{
			pthread_exit(NULL);
			return;
		}

		// Assign a job to be completed
		temp_job = Assign_Job(scheduler);

		// Execute the function with job's arguments
		(*(temp_job->job_to_do))(temp_job->job_arguments);

		// Completed the job and signal the barrier condition if necessary
		Complete_Job(scheduler);
		free(temp_job);
	}
	pthread_exit(NULL);
}