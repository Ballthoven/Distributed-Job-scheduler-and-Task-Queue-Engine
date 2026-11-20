package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class JobHandlerRegistry {

   public final Map<String, JobHandler> handlersByType = new HashMap<>();
    public JobHandlerRegistry(List<JobHandler> handlers) {
        for (JobHandler handler : handlers) {
            JobHandler previous = handlersByType.put(handler.type(), handler);
            if (previous != null) {
                throw new IllegalStateException("Two handlers registered for job type '" + handler.type() + "'");
            }
            log.info("Registered handler for job type '{}': {}", handler.type(), handler.getClass().getSimpleName());
        }
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlersByType.get(type));
    }
}
