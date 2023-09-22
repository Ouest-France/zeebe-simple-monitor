package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.ErrorRecordValue;
import io.zeebe.monitor.entity.ErrorEntity;
import io.zeebe.monitor.repository.ErrorRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ErrorImporter {

    @Autowired
    private ErrorRepository errorRepository;

    @KafkaListener(topics = "${kafka.zeebe.error}")
    public void handle(final ConsumerRecord<String, Record<ErrorRecordValue>> record) {

        Record<ErrorRecordValue> errorRecordValue = record.value();

        final var entity =
                errorRepository
                        .findById(errorRecordValue.getPosition())
                        .orElseGet(
                                () -> {
                                    final var newEntity = new ErrorEntity();
                                    newEntity.setPosition(errorRecordValue.getPosition());
                                    newEntity.setErrorEventPosition(errorRecordValue.getValue().getErrorEventPosition());
                                    newEntity.setProcessInstanceKey(errorRecordValue.getValue().getProcessInstanceKey());
                                    newEntity.setExceptionMessage(errorRecordValue.getValue().getExceptionMessage());
                                    newEntity.setStacktrace(errorRecordValue.getValue().getStacktrace());
                                    newEntity.setTimestamp(errorRecordValue.getTimestamp());
                                    return newEntity;
                                });

        errorRepository.save(entity);
    }

}
