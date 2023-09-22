package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.repository.JobRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class JobImporter {

    @Autowired
    private JobRepository jobRepository;

    @KafkaListener(topics = "${kafka.zeebe.job}")
    public void handle(final ConsumerRecord<String, Record<JobRecordValue>> record) {

        Record<JobRecordValue> jobRecordValueRecord = record.value();

        final JobIntent intent = JobIntent.valueOf(jobRecordValueRecord.getIntent().name());
        final long key = jobRecordValueRecord.getKey();
        final long timestamp = jobRecordValueRecord.getTimestamp();

        final JobEntity entity =
                jobRepository
                        .findById(key)
                        .orElseGet(
                                () -> {
                                    final JobEntity newEntity = new JobEntity();
                                    newEntity.setKey(key);
                                    newEntity.setProcessInstanceKey(jobRecordValueRecord.getValue().getProcessInstanceKey());
                                    newEntity.setElementInstanceKey(jobRecordValueRecord.getValue().getElementInstanceKey());
                                    newEntity.setJobType(jobRecordValueRecord.getValue().getType());
                                    return newEntity;
                                });

        entity.setState(intent.name().toLowerCase());
        entity.setTimestamp(timestamp);
        entity.setWorker(jobRecordValueRecord.getValue().getWorker());
        entity.setRetries(jobRecordValueRecord.getValue().getRetries());
        jobRepository.save(entity);
    }

}
