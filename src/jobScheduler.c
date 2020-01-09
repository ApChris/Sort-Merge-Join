#include <stdio.h>
#include <stdlib.h>
#include "../include/jobScheduler.h"

job_scheduler * Init_JobScheduler(){

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
	scheduler->jobs_queue = malloc(sizeof(job) * (scheduler->total_threads + 1));

	scheduler->threads = malloc(sizeof(pthread_t) * scheduler->total_threads);
	pthread_mutex_init((&scheduler->queue_thread_access), NULL);
	pthread_cond_init((&scheduler->barrier_cond_var), NULL);

	// semaphore is shared between threads of a process (0) and starting value = 0
	sem_init((&scheduler->queue_job_sem), 0, 0);

	for (uint64_t i = 0; i < scheduler->total_threads; i++)
	{
		if(pthread_create(&(scheduler->threads[i]), NULL, Thread_Routine, scheduler) != 0){
			perror("pthread_create error");
			exit(-1);
		}
		scheduler->jobs_left--;
	}

	return scheduler;
}


job * Next_Job(job_scheduler * scheduler){
	pthread_mutex_lock(&(scheduler->queue_thread_access));

	job * temp_job = scheduler->jobs_queue;
	scheduler->jobs_queue = scheduler->jobs_queue->next_job;

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

void Barrier(job_scheduler * scheduler){
	pthread_mutex_lock(&(scheduler->queue_thread_access));

	// wait for all jobs to finish and then signal the barrier condition variable
	while(scheduler->finished != 1){
		pthread_cond_wait(&(scheduler->barrier_cond_var),&(scheduler->queue_thread_access));
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
		temp_job = Next_Job(scheduler);

		// Execute the function with job's arguments
		(*(temp_job->job_to_do))(temp_job->job_arguments);

		// Completed the job and signal the barrier condition if necessary
		Complete_Job(scheduler);
		free(temp_job);
	}
	pthread_exit(NULL);
}

void Assign_Job(job_scheduler * scheduler, void * function, void * arguments){
	if (scheduler == NULL || function == NULL)
	{
		return;
	}

	pthread_mutex_lock(&(scheduler->queue_thread_access));

	job * temp_job;
	if ((temp_job = malloc(sizeof(job))) == NULL)
	{
		perror("Assign Job temp_job malloc");
		return;
	}

	temp_job->job_arguments = arguments;
	temp_job->job_to_do = function;
	temp_job->next_job = NULL;	

	// if queue is empty
	if (scheduler->jobs_queue == NULL)
	{
		scheduler->jobs_queue = temp_job;
	}
	else // else insert at front
	{
		temp_job->next_job = scheduler->jobs_queue;
		scheduler->jobs_queue = temp_job;
	}

	// signal a thread to execute the job
	sem_post(&(scheduler->queue_job_sem));

	pthread_mutex_unlock(&(scheduler->queue_thread_access));
}

void Destroy_JobScheduler(job_scheduler * scheduler){
	//the flag to identify if all jobs are finished
	scheduler->finished = 1;

	// wake every thread that waits at the queue_job_sem
	for (uint64_t i = 0; i < scheduler->total_threads; i++)
	{
		sem_post(&(scheduler->queue_job_sem));
	}

	// after waking, wait for every thread to exit before destroying
	for (uint64_t i = 0; i < scheduler->total_threads; i++)
	{
		pthread_join(scheduler->threads[i], NULL);
	}

	// free every mutex, cond, sem and memory
	pthread_mutex_destroy(&(scheduler->queue_thread_access));
	pthread_cond_destroy(&(scheduler->barrier_cond_var));
	sem_destroy(&(scheduler->queue_job_sem));
	free(scheduler->threads);
	free(scheduler);
}