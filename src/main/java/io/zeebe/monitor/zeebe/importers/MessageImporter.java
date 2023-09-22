package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.repository.MessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageImporter {

    @Autowired
    private MessageRepository messageRepository;

    @KafkaListener(topics = "${kafka.zeebe.message}")
    public void handle(final ConsumerRecord<String, Record<MessageRecordValue>> record) {
        Record<MessageRecordValue> messageRecordValueRecord = record.value();

        final MessageIntent intent = MessageIntent.valueOf(messageRecordValueRecord.getIntent().name());
        final long key = messageRecordValueRecord.getKey();
        final long timestamp = messageRecordValueRecord.getTimestamp();

        final MessageEntity entity =
                messageRepository
                        .findById(key)
                        .orElseGet(
                                () -> {
                                    final MessageEntity newEntity = new MessageEntity();
                                    newEntity.setKey(key);
                                    newEntity.setName(messageRecordValueRecord.getValue().getName());
                                    newEntity.setCorrelationKey(messageRecordValueRecord.getValue().getCorrelationKey());
                                    newEntity.setMessageId(messageRecordValueRecord.getValue().getMessageId());
                                    newEntity.setPayload(messageRecordValueRecord.getValue().getVariables().toString());
                                    return newEntity;
                                });

        entity.setState(intent.name().toLowerCase());
        entity.setTimestamp(timestamp);
        messageRepository.save(entity);
    }
}
