package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DistributedJobSchedulerAndTaskQueueEngineApplicationTests extends IntegrationTestBase{

	@Test
	void contextLoads() {
	}

}
