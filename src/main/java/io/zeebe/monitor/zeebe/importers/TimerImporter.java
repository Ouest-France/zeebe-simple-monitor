package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.TimerIntent;
import io.camunda.zeebe.protocol.record.value.TimerRecordValue;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.repository.TimerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TimerImporter {

  @Autowired private TimerRepository timerRepository;

    @KafkaListener(topics = "${kafka.zeebe.timer}")
    public void handle(final ConsumerRecord<String, Record<TimerRecordValue>> record) {

        Record<TimerRecordValue> timerRecordValue = record.value();
    final TimerIntent intent = TimerIntent.valueOf(timerRecordValue.getIntent().name());
    final long key = timerRecordValue.getKey();
    final long timestamp = timerRecordValue.getTimestamp();

    final TimerEntity entity =
        timerRepository
            .findById(key)
            .orElseGet(
                () -> {
                  final TimerEntity newEntity = new TimerEntity();
                  newEntity.setKey(key);
                  newEntity.setProcessDefinitionKey(timerRecordValue.getValue().getProcessDefinitionKey());
                  newEntity.setTargetElementId(timerRecordValue.getValue().getTargetElementId());
                  newEntity.setDueDate(timerRecordValue.getValue().getDueDate());
                  newEntity.setRepetitions(timerRecordValue.getValue().getRepetitions());

                  if (timerRecordValue.getValue().getProcessInstanceKey() > 0) {
                    newEntity.setProcessInstanceKey(timerRecordValue.getValue().getProcessInstanceKey());
                    newEntity.setElementInstanceKey(timerRecordValue.getValue().getElementInstanceKey());
                  }

                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    timerRepository.save(entity);
  }

}
