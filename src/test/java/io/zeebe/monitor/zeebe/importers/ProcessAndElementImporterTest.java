package io.zeebe.monitor.zeebe.importers;

import io.camunda.zeebe.protocol.record.ImmutableRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ImmutableProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.ZeebeRepositoryTest;
import io.zeebe.monitor.zeebe.ZeebeNotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@ContextConfiguration(
        classes = {ProcessImporter.class,
                ProcessInstanceImporter.class,
                ZeebeNotificationService.class}
)
public class ProcessAndElementImporterTest extends ZeebeRepositoryTest {

    @Autowired
    ProcessInstanceImporter processInstanceImporter;

    @Autowired
    ElementInstanceRepository elementInstanceRepository;

    @MockBean
    SimpMessagingTemplate simpMessagingTemplate;

    @Test
    public void only_storing_first_variable_event_prevents_duplicate_PartitionID_and_Position() {
        // given
        ImmutableRecord<ProcessInstanceRecordValue> processInstance1 = createElementInstanceWithId("first-elementId");
        processInstanceImporter.handle(new ConsumerRecord<>("", 1, 1, null, processInstance1));

        // when
        ImmutableRecord<ProcessInstanceRecordValue> processInstance2 = createElementInstanceWithId("second-elementId");
        processInstanceImporter.handle(new ConsumerRecord<>("", 1, 2, null, processInstance2));

        // then
        Iterable<ElementInstanceEntity> all = elementInstanceRepository.findAll();
        assertThat(all).hasSize(1);
        assertThat(all.iterator().next().getElementId()).isEqualTo("first-elementId");
    }

    private ImmutableRecord<ProcessInstanceRecordValue> createElementInstanceWithId(String elementId) {
        return ImmutableRecord.<ProcessInstanceRecordValue>builder()
                .withPartitionId(55555)
                .withPosition(333L)
                .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withValue(ImmutableProcessInstanceRecordValue.builder()
                        .withBpmnElementType(BpmnElementType.PROCESS)
                        .withElementId(elementId).build())
                .build();
    }

}
