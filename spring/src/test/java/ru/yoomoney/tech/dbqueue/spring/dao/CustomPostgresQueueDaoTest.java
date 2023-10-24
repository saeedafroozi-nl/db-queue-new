package ru.yoomoney.tech.dbqueue.spring.dao;

import java.time.Duration;
import java.util.List;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public CustomPostgresQueueDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.CUSTOM_SCHEMA),
                PostgresDatabaseInitializer.CUSTOM_TABLE_NAME, PostgresDatabaseInitializer.CUSTOM_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }
    
    @Test
    public void enqueueBatch_should_save_all_tasks() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        Duration executionDelay = Duration.ofHours(1L);
        
        executeInTransaction(() -> queueDao.enqueueBatch(location, List.of(
                EnqueueParams.create(payload).withExecutionDelay(executionDelay),
                EnqueueParams.create(payload).withExecutionDelay(executionDelay),
                EnqueueParams.create(payload).withExecutionDelay(executionDelay)
        )));
        jdbcTemplate.query(
                "select count(*) from " + tableName + 
                " where " + tableSchema.getQueueNameField() + "='" + location.getQueueId().asString() + "'", 
                rs -> {
                    MatcherAssert.assertThat(rs.next(), equalTo(true));
                    MatcherAssert.assertThat(rs.getInt(1), equalTo(3));
        
                    return new Object();
                }
        );
    }
    
}
