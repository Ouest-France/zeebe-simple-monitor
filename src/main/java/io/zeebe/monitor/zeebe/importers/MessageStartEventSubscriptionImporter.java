package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.camunda.zeebe.protocol.record.value.MessageStartEventSubscriptionRecordValue;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class MessageStartEventSubscriptionImporter {

    @Autowired
    private MessageSubscriptionRepository messageSubscriptionRepository;

    @KafkaListener(topics = "${kafka.zeebe.message-start-event-subscription}")
    public void handle(final ConsumerRecord<String, Record<MessageStartEventSubscriptionRecordValue>> record) {
        Record<MessageStartEventSubscriptionRecordValue> messageStartEventSubscriptionRecordValue = record.value();

        final MessageStartEventSubscriptionIntent intent =
                MessageStartEventSubscriptionIntent.valueOf(messageStartEventSubscriptionRecordValue.getIntent().name());
        final long timestamp = messageStartEventSubscriptionRecordValue.getTimestamp();

        final MessageSubscriptionEntity entity =
                messageSubscriptionRepository
                        .findByProcessDefinitionKeyAndMessageName(
                                messageStartEventSubscriptionRecordValue.getValue().getProcessDefinitionKey(), messageStartEventSubscriptionRecordValue.getValue().getMessageName())
                        .orElseGet(
                                () -> {
                                    final MessageSubscriptionEntity newEntity = new MessageSubscriptionEntity();
                                    newEntity.setId(UUID.randomUUID().toString());
                                    newEntity.setMessageName(messageStartEventSubscriptionRecordValue.getValue().getMessageName());
                                    newEntity.setProcessDefinitionKey(messageStartEventSubscriptionRecordValue.getValue().getProcessDefinitionKey());
                                    newEntity.setTargetFlowNodeId(messageStartEventSubscriptionRecordValue.getValue().getStartEventId());
                                    return newEntity;
                                });

        entity.setState(intent.name().toLowerCase());
        entity.setTimestamp(timestamp);
        messageSubscriptionRepository.save(entity);
    }
}
