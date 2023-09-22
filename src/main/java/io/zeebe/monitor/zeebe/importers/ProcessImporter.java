package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.deployment.Process;
import io.zeebe.monitor.entity.ProcessEntity;
import io.zeebe.monitor.repository.ProcessRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class ProcessImporter {

  @Autowired
  private ProcessRepository processRepository;

  @KafkaListener(topics = "${kafka.zeebe.process}")
  public void handle(final ConsumerRecord<String, Record<Process>> record) {
    Record<Process> recordZeebe = record.value();

    final int partitionId = recordZeebe.getPartitionId();

    if (partitionId != Protocol.DEPLOYMENT_PARTITION) {
      // ignore process event on other partitions to avoid duplicates
      return;
    }

    final ProcessEntity entity = new ProcessEntity();
    entity.setKey(recordZeebe.getValue().getProcessDefinitionKey());
    entity.setBpmnProcessId(recordZeebe.getValue().getBpmnProcessId());
    entity.setVersion(recordZeebe.getValue().getVersion());
    entity.setResource(new String(recordZeebe.getValue().getResource(), StandardCharsets.UTF_8));
    entity.setTimestamp(recordZeebe.getTimestamp());
    processRepository.save(entity);
  }


}
