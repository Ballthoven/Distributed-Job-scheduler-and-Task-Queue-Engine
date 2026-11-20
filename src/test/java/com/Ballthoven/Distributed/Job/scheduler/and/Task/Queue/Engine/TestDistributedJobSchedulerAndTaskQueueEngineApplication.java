package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine;

import org.springframework.boot.SpringApplication;

public class TestDistributedJobSchedulerAndTaskQueueEngineApplication {

	public static void main(String[] args) {
		SpringApplication.from(DistributedJobSchedulerAndTaskQueueEngineApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
