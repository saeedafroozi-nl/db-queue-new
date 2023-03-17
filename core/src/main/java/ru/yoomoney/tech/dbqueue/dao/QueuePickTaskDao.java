package ru.yoomoney.tech.dbqueue.dao;

import ru.yoomoney.tech.dbqueue.api.TaskRecord;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Database access object to pick up tasks in the queue.
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueuePickTaskDao {

    /**
     * Pick tasks from a queue
     *
     * @return list of tasks data or empty if not found
     */
    @Nonnull
    List<TaskRecord> pickTasks();
}
