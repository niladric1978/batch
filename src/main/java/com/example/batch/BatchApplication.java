package com.example.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SpringBootApplication
public class BatchApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(BatchApplication.class);

	public static void main(final String[] args) {
		// Spring Java config
		final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(SpringbatchPartitionConfig.class);
		context.refresh();

		final JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
		final Job job = (Job) context.getBean("partitionerJob");
		LOGGER.info("Starting the batch job");
		try {
			final JobExecution execution = jobLauncher.run(job, new JobParameters());
			LOGGER.info("Job Status : {}", execution.getStatus());
		} catch (final Exception e) {
			e.printStackTrace();
			LOGGER.error("Job failed {}", e.getMessage());
		}
	}
}