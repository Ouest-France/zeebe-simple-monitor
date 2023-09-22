package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.ProcessInstanceEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.zeebe.ZeebeNotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProcessInstanceImporter {

    @Autowired
    private ProcessRepository processRepository;
    @Autowired
    private ProcessInstanceRepository processInstanceRepository;
    @Autowired
    private ElementInstanceRepository elementInstanceRepository;

    @Autowired
    private ZeebeNotificationService notificationService;


    @KafkaListener(topics = "${kafka.zeebe.process-instance}")
    public void handle(final ConsumerRecord<String, Record<ProcessInstanceRecordValue>> record) {
        Record<ProcessInstanceRecordValue> recordZeebe = record.value();
        if (recordZeebe.getValue().getProcessInstanceKey() == recordZeebe.getKey()) {
            addOrUpdateProcessInstance(recordZeebe);
        }
        addElementInstance(recordZeebe);
    }

    private void addOrUpdateProcessInstance(final Record<ProcessInstanceRecordValue> record) {

        final long timestamp = record.getTimestamp();
        final long processInstanceKey = record.getValue().getProcessInstanceKey();

        final ProcessInstanceEntity entity =
                processInstanceRepository
                        .findById(processInstanceKey)
                        .orElseGet(
                                () -> {
                                    final ProcessInstanceEntity newEntity = new ProcessInstanceEntity();
                                    newEntity.setPartitionId(record.getPartitionId());
                                    newEntity.setKey(processInstanceKey);
                                    newEntity.setBpmnProcessId(record.getValue().getBpmnProcessId());
                                    newEntity.setVersion(record.getValue().getVersion());
                                    newEntity.setProcessDefinitionKey(record.getValue().getProcessDefinitionKey());
                                    newEntity.setParentProcessInstanceKey(record.getValue().getParentProcessInstanceKey());
                                    newEntity.setParentElementInstanceKey(record.getValue().getParentElementInstanceKey());
                                    return newEntity;
                                });

        if (record.getIntent() == ProcessInstanceIntent.ELEMENT_ACTIVATED) {
            entity.setState("Active");
            entity.setStart(timestamp);
            processInstanceRepository.save(entity);

            notificationService.sendCreatedProcessInstance(
                    record.getValue().getProcessInstanceKey(), record.getValue().getProcessDefinitionKey());

        } else if (record.getIntent() == ProcessInstanceIntent.ELEMENT_COMPLETED) {
            entity.setState("Completed");
            entity.setEnd(timestamp);
            processInstanceRepository.save(entity);

            notificationService.sendEndedProcessInstance(
                    record.getValue().getProcessInstanceKey(), record.getValue().getProcessDefinitionKey());

        } else if (record.getIntent() == ProcessInstanceIntent.ELEMENT_TERMINATED) {
            entity.setState("Terminated");
            entity.setEnd(timestamp);
            processInstanceRepository.save(entity);

            notificationService.sendEndedProcessInstance(
                    record.getValue().getProcessInstanceKey(), record.getValue().getProcessDefinitionKey());
        }
    }

    private void addElementInstance(final Record<ProcessInstanceRecordValue> record) {
        final ElementInstanceEntity entity = new ElementInstanceEntity();
        entity.setPartitionId(record.getPartitionId());
        entity.setPosition(record.getPosition());
        if (!elementInstanceRepository.existsById(entity.getGeneratedIdentifier())) {
            entity.setKey(record.getKey());
            entity.setIntent(record.getIntent().name());
            entity.setTimestamp(record.getTimestamp());
            entity.setProcessInstanceKey(record.getValue().getProcessInstanceKey());
            entity.setElementId(record.getValue().getElementId());
            entity.setFlowScopeKey(record.getValue().getFlowScopeKey());
            entity.setProcessDefinitionKey(record.getValue().getProcessDefinitionKey());
            entity.setBpmnElementType(record.getValue().getBpmnElementType().name());
            elementInstanceRepository.save(entity);
            notificationService.sendUpdatedProcessInstance(record.getValue().getProcessInstanceKey(), record.getValue().getProcessDefinitionKey());
        }
    }

}
