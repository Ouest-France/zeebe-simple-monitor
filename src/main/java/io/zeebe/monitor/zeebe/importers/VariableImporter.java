package io.zeebe.monitor.zeebe.importers;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.repository.VariableRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class VariableImporter {

    @Autowired
    private VariableRepository variableRepository;

    @KafkaListener(topics = "${kafka.zeebe.variable}")
    public void handle(final ConsumerRecord<String, Record<VariableRecordValue>> consumerRecord) throws JsonProcessingException {
        Record<VariableRecordValue> variableRecordValue = consumerRecord.value();

        final VariableEntity newVariable = new VariableEntity();
        newVariable.setPosition(variableRecordValue.getPosition());
        newVariable.setPartitionId(variableRecordValue.getPartitionId());
        if (!variableRepository.existsById(newVariable.getGeneratedIdentifier())) {
            newVariable.setTimestamp(variableRecordValue.getTimestamp());
            newVariable.setProcessInstanceKey(variableRecordValue.getValue().getProcessInstanceKey());
            newVariable.setName(variableRecordValue.getValue().getName());
            newVariable.setValue(variableRecordValue.getValue().getValue());
            newVariable.setScopeKey(variableRecordValue.getValue().getScopeKey());
            newVariable.setState(variableRecordValue.getIntent().name().toLowerCase());
            variableRepository.save(newVariable);
        }
    }

}
