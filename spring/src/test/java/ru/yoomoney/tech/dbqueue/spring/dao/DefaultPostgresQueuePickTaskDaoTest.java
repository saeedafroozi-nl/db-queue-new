package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public DefaultPostgresQueuePickTaskDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) -> new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.DEFAULT_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                PostgresDatabaseInitializer.DEFAULT_TABLE_NAME, PostgresDatabaseInitializer.DEFAULT_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }

    @Test
    public void should_pick_tasks_batch() {
        QueueLocation location = generateUniqueLocation();
        PollSettings pollSettings = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(2).build();
        FailureSettings failureSettings = FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build();
        QueuePickTaskDao pickTaskDao = new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                PostgresDatabaseInitializer.DEFAULT_SCHEMA, location, failureSettings, pollSettings);

        executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));
        executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        List<TaskRecord> taskRecords = executeInTransaction(pickTaskDao::pickTasks);
        Assert.assertThat(taskRecords.size(), equalTo(2));
    }
}
