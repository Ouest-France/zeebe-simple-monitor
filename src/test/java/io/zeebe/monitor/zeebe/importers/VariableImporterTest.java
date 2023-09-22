package io.zeebe.monitor.zeebe.importers;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.camunda.zeebe.protocol.record.ImmutableRecord;
import io.camunda.zeebe.protocol.record.value.ImmutableVariableRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.repository.VariableRepository;
import io.zeebe.monitor.repository.ZeebeRepositoryTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@ContextConfiguration(
    classes = {VariableImporter.class}
)
public class VariableImporterTest extends ZeebeRepositoryTest {

  @Autowired
  VariableImporter variableImporter;

  @Autowired
  VariableRepository variableRepository;

  @Test
  public void only_storing_first_variable_event_prevents_duplicate_PartitionID_and_Position() throws JsonProcessingException {
    // given
    ImmutableRecord<VariableRecordValue> record1 = createVariableRecordWithName("first-variable");
    variableImporter.handle(new ConsumerRecord<>("",1,1,null,record1));

    // when
    ImmutableRecord<VariableRecordValue>  record2 = createVariableRecordWithName("second-variable");
    variableImporter.handle(new ConsumerRecord<>("",1,1,null,record2));

    // then
    Iterable<VariableEntity> all = variableRepository.findAll();
    assertThat(all).hasSize(1);
    assertThat(all.iterator().next().getName()).isEqualTo("first-variable");
  }

  private ImmutableRecord<VariableRecordValue> createVariableRecordWithName(String name) {
    return ImmutableRecord.<VariableRecordValue>builder()
            .withPartitionId(123)
            .withPosition(456L)
            .withValue(ImmutableVariableRecordValue.builder()
            .withName(name).build()).build();
  }
}
