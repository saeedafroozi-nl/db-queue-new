package ru.yoomoney.tech.dbqueue.internal.runner;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.stub.TestFixtures;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerInTransactionTest {

    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(new QueueId("test_queue1")).build();

    @Test
    public void should_wait_notasktimeout_when_no_task_found() throws Exception {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        when(taskPicker.pickTasks()).thenReturn(List.of());
        TaskProcessor taskProcessor = mock(TaskProcessor.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());

        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus status = new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.SKIPPED));

        verifyNoInteractions(queueShard);
        verify(taskPicker).pickTasks();
        verifyNoInteractions(taskProcessor);
    }

    @Test
    public void should_wait_betweentasktimeout_when_task_found() throws Exception {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        TaskRecord taskRecord = TaskRecord.builder().build();
        when(taskPicker.pickTasks()).thenReturn(List.of(taskRecord));
        TaskProcessor taskProcessor = mock(TaskProcessor.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());


        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus queueProcessingStatus = new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer);

        assertThat(queueProcessingStatus, equalTo(QueueProcessingStatus.PROCESSED));

        verify(queueShard).getDatabaseAccessLayer();
        verify(taskPicker).pickTasks();
        verify(taskProcessor).processTask(queueConsumer, taskRecord);
    }
}