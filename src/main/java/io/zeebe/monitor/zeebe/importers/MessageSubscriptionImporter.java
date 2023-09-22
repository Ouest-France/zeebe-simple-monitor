package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.value.MessageSubscriptionRecordValue;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class MessageSubscriptionImporter {

    @Autowired
    private MessageSubscriptionRepository messageSubscriptionRepository;

    @KafkaListener(topics = "${kafka.zeebe.message-subscription}")
    public void handle(final ConsumerRecord<String, Record<MessageSubscriptionRecordValue>> record) {
        Record<MessageSubscriptionRecordValue> messageSubscriptionRecordValue = record.value();

        final MessageSubscriptionIntent intent =
                MessageSubscriptionIntent.valueOf(messageSubscriptionRecordValue.getIntent().name());
        final long timestamp = messageSubscriptionRecordValue.getTimestamp();

        final MessageSubscriptionEntity entity =
                messageSubscriptionRepository
                        .findByElementInstanceKeyAndMessageName(
                                messageSubscriptionRecordValue.getValue().getElementInstanceKey(), messageSubscriptionRecordValue.getValue().getMessageName())
                        .orElseGet(
                                () -> {
                                    final MessageSubscriptionEntity newEntity = new MessageSubscriptionEntity();
                                    newEntity.setId(UUID.randomUUID().toString());
                                    newEntity.setElementInstanceKey(messageSubscriptionRecordValue.getValue().getElementInstanceKey());
                                    newEntity.setMessageName(messageSubscriptionRecordValue.getValue().getMessageName());
                                    newEntity.setCorrelationKey(messageSubscriptionRecordValue.getValue().getCorrelationKey());
                                    newEntity.setProcessInstanceKey(messageSubscriptionRecordValue.getValue().getProcessInstanceKey());
                                    return newEntity;
                                });

        entity.setState(intent.name().toLowerCase());
        entity.setTimestamp(timestamp);
        messageSubscriptionRepository.save(entity);
    }


}
