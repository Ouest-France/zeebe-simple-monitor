package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.repository.IncidentRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IncidentImporter {

  @Autowired private IncidentRepository incidentRepository;
    @KafkaListener(topics = "${kafka.zeebe.incident}")
    public void handle(final ConsumerRecord<String, Record<IncidentRecordValue>> record) {

        Record<IncidentRecordValue> incidentRecordValueRecord = record.value();

    final IncidentIntent intent = IncidentIntent.valueOf(incidentRecordValueRecord.getIntent().name());

    final IncidentEntity entity =
        incidentRepository
            .findById(incidentRecordValueRecord.getKey())
            .orElseGet(
                () -> {
                  final IncidentEntity newEntity = new IncidentEntity();
                  newEntity.setKey(incidentRecordValueRecord.getKey());
                  newEntity.setBpmnProcessId(incidentRecordValueRecord.getValue().getBpmnProcessId());
                  newEntity.setProcessDefinitionKey(incidentRecordValueRecord.getValue().getProcessDefinitionKey());
                  newEntity.setProcessInstanceKey(incidentRecordValueRecord.getValue().getProcessInstanceKey());
                  newEntity.setElementInstanceKey(incidentRecordValueRecord.getValue().getElementInstanceKey());
                  newEntity.setJobKey(incidentRecordValueRecord.getValue().getJobKey());
                  newEntity.setErrorType(incidentRecordValueRecord.getValue().getErrorType().name());
                  newEntity.setErrorMessage(incidentRecordValueRecord.getValue().getErrorMessage());
                  return newEntity;
                });

    if (intent == IncidentIntent.CREATED) {
      entity.setCreated(incidentRecordValueRecord.getTimestamp());
      incidentRepository.save(entity);

    } else if (intent == IncidentIntent.RESOLVED) {
      entity.setResolved(incidentRecordValueRecord.getTimestamp());
      incidentRepository.save(entity);
    }
  }

}
