package ru.yoomoney.tech.dbqueue.internal.runner;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Исполнитель задач очереди в режиме
 * {@link ProcessingMode#USE_EXTERNAL_EXECUTOR}
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class QueueRunnerInExternalExecutor implements QueueRunner {

    @Nonnull
    private final TaskPicker taskPicker;
    @Nonnull
    private final TaskProcessor taskProcessor;
    @Nonnull
    private final Executor executor;

    /**
     * Конструктор
     *
     * @param taskPicker    выборщик задачи
     * @param taskProcessor обработчик задачи
     * @param executor      исполнитель задачи
     */
    QueueRunnerInExternalExecutor(@Nonnull TaskPicker taskPicker,
                                  @Nonnull TaskProcessor taskProcessor,
                                  @Nonnull Executor executor) {
        this.taskPicker = requireNonNull(taskPicker);
        this.taskProcessor = requireNonNull(taskProcessor);
        this.executor = requireNonNull(executor);
    }

    @Override
    @Nonnull
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        List<TaskRecord> taskRecords = taskPicker.pickTasks();
        if (taskRecords.isEmpty()) {
            return QueueProcessingStatus.SKIPPED;
        }
        taskRecords.forEach(taskRecord ->
                executor.execute(() -> taskProcessor.processTask(queueConsumer, taskRecord)));
        return QueueProcessingStatus.PROCESSED;
    }

}
