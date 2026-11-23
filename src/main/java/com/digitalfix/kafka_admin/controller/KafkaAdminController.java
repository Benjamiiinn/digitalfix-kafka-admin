package com.digitalfix.kafka_admin.controller;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.digitalfix.kafka_admin.dto.BrokerInfo;
import com.digitalfix.kafka_admin.dto.ConsumerGroupInfo;
import com.digitalfix.kafka_admin.dto.ConsumerLagInfo;
import com.digitalfix.kafka_admin.dto.CreateTopicRequest;
import com.digitalfix.kafka_admin.dto.TopicConfigInfo;
import com.digitalfix.kafka_admin.dto.TopicInfo;
import com.digitalfix.kafka_admin.dto.UpdatePartitionsRequest;
import com.digitalfix.kafka_admin.dto.UpdateTopicConfigRequest;
import com.digitalfix.kafka_admin.service.KafkaAdminService;

import jakarta.validation.Valid;

@RestController
@RequestMapping ("/api/kafka") 
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/topics")
    public List<String> listTopics()
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        return service.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicInfo getTopic(
            @PathVariable String name)
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        return service.getTopic(name);
    }

    @GetMapping("/topics/{name}/configs")
    public TopicConfigInfo getTopicConfig(
            @PathVariable String name)
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        return service.getTopicConfig(name);
    }

    @PutMapping("/topics/{name}/configs")
    public void updateTopicConfig(
            @PathVariable String name,
            @Valid @RequestBody UpdateTopicConfigRequest request)
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        service.updateTopicConfig(name, request);
    }

    @PostMapping("/topics/{name}/partitions")
    public void increasePartitions(
            @PathVariable String name,
            @Valid @RequestBody UpdatePartitionsRequest request)
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        service.increasePartitions(name, request);
    }

    @GetMapping("/cluster")
    public List<BrokerInfo> getCluster()
            throws InterruptedException,
                   ExecutionException,
                   TimeoutException {

        return service.getBrokers();
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public void createTopic(
            @Valid @RequestBody CreateTopicRequest request)
            throws Exception {

        service.createTopic(request);
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable String name)
            throws Exception {

        service.deleteTopic(name);

        return ResponseEntity.noContent().build();
    }

    @GetMapping("/consumer-groups")
    public List<ConsumerGroupInfo> listConsumerGroups()
            throws Exception {

        return service.listConsumerGroups();
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public ConsumerLagInfo getConsumerLag(
            @PathVariable String groupId)
            throws Exception {

        return service.getConsumerLag(groupId);
    }
}
