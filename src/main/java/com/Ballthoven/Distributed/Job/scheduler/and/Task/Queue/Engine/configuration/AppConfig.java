package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.configuration;


import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker.BoundedWorkerPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Slf4j
@Configuration
@EnableScheduling
@EnableConfigurationProperties(WorkerProperties.class)
public class AppConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

     @Bean(destroyMethod = "close")
     BoundedWorkerPool workerPool(WorkerProperties props) {
        log.info("Creating worker pool: {} threads, queue capacity {}, lease {}",
                props.threads(), props.queueCapacity(), props.lease());
        return new BoundedWorkerPool("jobforge", props.threads(), props.queueCapacity(), props.shutdownTimeout());
    }
}
