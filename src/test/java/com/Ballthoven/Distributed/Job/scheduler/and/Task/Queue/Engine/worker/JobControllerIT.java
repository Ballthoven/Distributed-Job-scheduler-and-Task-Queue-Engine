package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;



import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.IntegrationTestBase;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobRepository;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@AutoConfigureMockMvc
public class JobControllerIT extends IntegrationTestBase {
    @Autowired
    MockMvc mvc;
    @Autowired
    JobRepository jobRepository;

    @BeforeEach
    void cleanTable() {
        jobRepository.deleteAll();
    }

    @Test
    void submitReturns202WithLocationAndAPendingJob() throws Exception {
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"email\",\"payload\":\"hello\",\"priority\":5}"))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.type").value("email"))
                .andExpect(jsonPath("$.priority").value(5))
                .andExpect(jsonPath("$.attempts").value(0))
                .andExpect(jsonPath("$.maxAttempts").value(5));
    }

    @Test
    void sameIdempotencyKeyReturnsTheSameJobAndCreatesOnlyOne() throws Exception {
        String body = "{\"type\":\"email\",\"payload\":\"hello\"}";

        String first = mvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", "order-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();

        String second = mvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", "order-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();

        String firstId = JsonPath.read(first, "$.id");
        String secondId = JsonPath.read(second, "$.id");
        assertThat(secondId).isEqualTo(firstId);
        assertThat(jobRepository.count()).isEqualTo(1);
    }

    @Test
    void requestsWithoutAKeyAlwaysCreateNewJobs() throws Exception {
        String body = "{\"type\":\"email\"}";
        for (int i = 0; i < 3; i++) {
            mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isAccepted());
        }
        assertThat(jobRepository.count()).isEqualTo(3);
    }

    @Test
    void blankTypeIsRejectedWith400() throws Exception {
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"  \"}"))
                .andExpect(status().isBadRequest());
        assertThat(jobRepository.count()).isZero();
    }

    @Test
    void outOfRangePriorityIsRejectedWith400() throws Exception {
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"email\",\"priority\":500}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void overlongIdempotencyKeyIsRejectedWith400() throws Exception {
        mvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", "k".repeat(201))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"email\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getReturnsTheSubmittedJob() throws Exception {
        String created = mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"email\",\"payload\":\"hello\"}"))
                .andReturn().getResponse().getContentAsString();
        String id = JsonPath.read(created, "$.id");

        mvc.perform(get("/api/jobs/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.payload").value("hello"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void getUnknownJobReturns404() throws Exception {
        mvc.perform(get("/api/jobs/" + UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    void getWithAMalformedIdReturns400() throws Exception {
        mvc.perform(get("/api/jobs/not-a-uuid"))
                .andExpect(status().isBadRequest());
    }
}
