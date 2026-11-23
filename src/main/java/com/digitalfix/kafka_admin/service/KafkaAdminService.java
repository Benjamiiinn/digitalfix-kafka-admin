package com.digitalfix.kafka_admin.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Service;

import com.digitalfix.kafka_admin.dto.BrokerInfo;
import com.digitalfix.kafka_admin.dto.ConsumerGroupInfo;
import com.digitalfix.kafka_admin.dto.ConsumerLagInfo;
import com.digitalfix.kafka_admin.dto.CreateTopicRequest;
import com.digitalfix.kafka_admin.dto.TopicConfigInfo;
import com.digitalfix.kafka_admin.dto.TopicInfo;
import com.digitalfix.kafka_admin.dto.UpdatePartitionsRequest;
import com.digitalfix.kafka_admin.dto.UpdateTopicConfigRequest;

@Service
public class KafkaAdminService {

        private final AdminClient adminClient;

        public KafkaAdminService(AdminClient adminClient) {
                this.adminClient = adminClient;
        }

        public List<String> listTopics()
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                return adminClient.listTopics()
                                .names()
                                .get(15, TimeUnit.SECONDS)
                                .stream()
                                .sorted()
                                .toList();
        }

        public TopicInfo getTopic(String name)
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                TopicDescription description = adminClient.describeTopics(List.of(name))
                                .allTopicNames()
                                .get(15, TimeUnit.SECONDS)
                                .get(name);

                if (description == null) {
                        throw new IllegalArgumentException(
                                        "No se encontró el tópico: " + name);
                }

                List<TopicInfo.PartitionInfo> partitions = description.partitions()
                                .stream()
                                .map(this::mapPartition)
                                .toList();

                return new TopicInfo(
                                description.name(),
                                partitions.size(),
                                partitions);
        }

        public TopicConfigInfo getTopicConfig(String name)
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                ConfigResource resource = new ConfigResource(
                                ConfigResource.Type.TOPIC,
                                name);

                Config config = adminClient.describeConfigs(List.of(resource))
                                .all()
                                .get(15, TimeUnit.SECONDS)
                                .get(resource);

                Map<String, String> values = new HashMap<>();

                for (ConfigEntry entry : config.entries()) {
                        if (RELEVANT_TOPIC_CONFIGS.contains(entry.name())) {
                                values.put(entry.name(), entry.value());
                        }
                }

                return new TopicConfigInfo(name, values);
        }

        public void updateTopicConfig(
                        String name,
                        UpdateTopicConfigRequest request)
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                if (request.retentionMs() == null
                                && request.cleanupPolicy() == null) {
                        throw new IllegalArgumentException(
                                        "Debe indicar retentionMs o cleanupPolicy");
                }

                List<AlterConfigOp> operations = new ArrayList<>();

                if (request.retentionMs() != null) {
                        operations.add(new AlterConfigOp(
                                        new ConfigEntry(
                                                        "retention.ms",
                                                        request.retentionMs().toString()),
                                        AlterConfigOp.OpType.SET));
                }

                if (request.cleanupPolicy() != null) {
                        operations.add(new AlterConfigOp(
                                        new ConfigEntry(
                                                        "cleanup.policy",
                                                        request.cleanupPolicy()),
                                        AlterConfigOp.OpType.SET));
                }

                ConfigResource resource = new ConfigResource(
                                ConfigResource.Type.TOPIC,
                                name);

                adminClient.incrementalAlterConfigs(
                                Map.of(resource, operations))
                                .all()
                                .get(15, TimeUnit.SECONDS);
        }

        public void increasePartitions(
                        String name,
                        UpdatePartitionsRequest request)
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                TopicDescription description = adminClient.describeTopics(List.of(name))
                                .allTopicNames()
                                .get(15, TimeUnit.SECONDS)
                                .get(name);

                if (description == null) {
                        throw new IllegalArgumentException(
                                        "No se encontró el tópico: " + name);
                }

                int current = description.partitions().size();

                if (request.partitions() <= current) {
                        throw new IllegalArgumentException(
                                        "Las particiones deben ser mayores a las actuales ("
                                                        + current + ")");
                }

                adminClient.createPartitions(
                                Map.of(name,
                                                NewPartitions.increaseTo(
                                                                request.partitions())))
                                .all()
                                .get(15, TimeUnit.SECONDS);
        }

        public List<BrokerInfo> getBrokers()
                        throws InterruptedException,
                        ExecutionException,
                        TimeoutException {

                return adminClient.describeCluster()
                                .nodes()
                                .get(15, TimeUnit.SECONDS)
                                .stream()
                                .map(node -> new BrokerInfo(
                                                node.id(),
                                                node.host(),
                                                node.port()))
                                .sorted(
                                                java.util.Comparator.comparingInt(
                                                                BrokerInfo::id))
                                .toList();
        }

        private TopicInfo.PartitionInfo mapPartition(
                        TopicPartitionInfo partition) {

                int leaderId = partition.leader() != null
                                ? partition.leader().id()
                                : -1;

                List<Integer> replicas = partition.replicas()
                                .stream()
                                .map(Node::id)
                                .toList();

                List<Integer> isr = partition.isr()
                                .stream()
                                .map(Node::id)
                                .toList();

                return new TopicInfo.PartitionInfo(
                                partition.partition(),
                                leaderId,
                                replicas,
                                isr);
        }

        private static final Set<String> RELEVANT_TOPIC_CONFIGS = Set.of(
                        "retention.ms",
                        "cleanup.policy",
                        "min.insync.replicas",
                        "segment.bytes",
                        "max.message.bytes",
                        "compression.type");

        private static final Set<String> PROTECTED_TOPICS = Set.of(
                        "digitalfix-eventos",
                        "digitalfix-auditoria",
                        "digitalfix-eventos.DLT",
                        "digitalfix-auditoria.DLT");

        public void createTopic(CreateTopicRequest request)
                        throws Exception {

                if (request.name().startsWith("__")) {
                        throw new IllegalArgumentException(
                                        "No se permiten nombres reservados de Kafka");
                }

                NewTopic topic = new NewTopic(
                                request.name(),
                                request.partitions(),
                                request.replicationFactor());

                try {
                        adminClient.createTopics(List.of(topic))
                                        .all()
                                        .get(15, TimeUnit.SECONDS);

                } catch (ExecutionException ex) {

                        if (ex.getCause() instanceof TopicExistsException) {
                                throw new IllegalStateException(
                                                "El tópico ya existe: " + request.name());
                        }

                        throw ex;
                }
        }

        public void deleteTopic(String name)
                        throws Exception {

                if (name.startsWith("__")) {
                        throw new IllegalArgumentException(
                                        "No se pueden eliminar tópicos internos");
                }

                if (PROTECTED_TOPICS.contains(name)) {
                        throw new IllegalArgumentException(
                                        "No se permite eliminar un tópico protegido: " + name);
                }

                adminClient.deleteTopics(List.of(name))
                                .all()
                                .get(15, TimeUnit.SECONDS);
        }


        public List<ConsumerGroupInfo> listConsumerGroups()
                throws Exception {

        return adminClient.listConsumerGroups()
                .all()
                .get(15, TimeUnit.SECONDS)
                .stream()
                .map(group -> new ConsumerGroupInfo(
                        group.groupId(),
                        group.groupState()
                                .map(Enum::name)
                                .orElse("UNKNOWN")
                ))
                .sorted(
                        Comparator.comparing(
                                ConsumerGroupInfo::groupId
                        )
                )
                .toList();
        }


        public ConsumerLagInfo getConsumerLag(String groupId)
                throws Exception {

                // 1. Obtener los offsets confirmados por el grupo.
                Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                        adminClient.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(15, TimeUnit.SECONDS);

                if (committedOffsets.isEmpty()) {
                        return new ConsumerLagInfo(
                                groupId,
                                0,
                                List.of()
                        );
                }

                // 2. Solicitar el offset final de cada partición.
                Map<TopicPartition, OffsetSpec> offsetRequests =
                        new HashMap<>();

                for (TopicPartition partition : committedOffsets.keySet()) {
                        offsetRequests.put(
                                partition,
                                OffsetSpec.latest()
                        );
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
                        endOffsets =
                        adminClient.listOffsets(offsetRequests)
                                .all()
                                .get(15, TimeUnit.SECONDS);

                // 3. Calcular el lag por partición.
                List<ConsumerLagInfo.PartitionLagInfo> partitions =
                        new ArrayList<>();

                long totalLag = 0;

                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry
                        : committedOffsets.entrySet()) {

                        TopicPartition partition = entry.getKey();

                        long committedOffset = entry.getValue().offset();

                        long endOffset = endOffsets
                                .get(partition)
                                .offset();

                        long lag = Math.max(
                                0,
                                endOffset - committedOffset
                        );

                        totalLag += lag;

                        partitions.add(
                                new ConsumerLagInfo.PartitionLagInfo(
                                        partition.topic(),
                                        partition.partition(),
                                        committedOffset,
                                        endOffset,
                                        lag
                                )
                        );
                }

                partitions.sort(
                        Comparator.comparing(
                                ConsumerLagInfo.PartitionLagInfo::topic
                        ).thenComparingInt(
                                ConsumerLagInfo.PartitionLagInfo::partition
                        )
                );

                return new ConsumerLagInfo(
                        groupId,
                        totalLag,
                        partitions
                );
        }


}
