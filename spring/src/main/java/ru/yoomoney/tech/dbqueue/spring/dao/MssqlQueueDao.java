package ru.yoomoney.tech.dbqueue.spring.dao;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to manage tasks in the queue for Microsoft SQL server database type.
 *
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class MssqlQueueDao implements QueueDao {

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final NamedParameterJdbcTemplate jdbcTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Constructor
     *
     * @param jdbcTemplate     Reference to Spring JDBC template.
     * @param queueTableSchema Queue table scheme.
     */
    public MssqlQueueDao(@Nonnull JdbcOperations jdbcTemplate,
                         @Nonnull QueueTableSchema queueTableSchema) {
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
    }

    @Override
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().toMillis());

        queueTableSchema.getExtFields().forEach(paramName -> params.addValue(paramName, null));
        enqueueParams.getExtData().forEach(params::addValue);
        return requireNonNull(jdbcTemplate.queryForObject(
                enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql), params, Long.class));
    }
    
    @Override
    public void enqueueBatch(@Nonnull QueueLocation location, @Nonnull List<EnqueueParams<String>> enqueueParams) {
        throw new UnsupportedOperationException("batch enqueue is not supported for this type of database");
    }

    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);

        int updatedRows = jdbcTemplate.update(deleteSqlCache.computeIfAbsent(location, this::createDeleteSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.toMillis()));
        return updatedRows != 0;
    }

    private String createEnqueueSql(@Nonnull QueueLocation location) {
        return "INSERT INTO " + location.getTableName() + "(" +
                (location.getIdSequence().map(ignored -> queueTableSchema.getIdField() + ",").orElse("")) +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") OUTPUT inserted." + queueTableSchema.getIdField() + " VALUES " +
                "(" + location.getIdSequence().map(seq -> "NEXT VALUE FOR " + seq + ", ").orElse("") +
                ":queueName, :payload, dateadd(ms, :executionDelay, SYSDATETIMEOFFSET()), 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ")";
    }

    private String createDeleteSql(@Nonnull QueueLocation location) {
        return "DELETE FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getIdField() + " = :id";
    }

    private String createReenqueueSql(@Nonnull QueueLocation location) {
        return "UPDATE " + location.getTableName() + " SET " + queueTableSchema.getNextProcessAtField() +
                " = dateadd(ms, :executionDelay, SYSDATETIMEOFFSET()), " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() +
                " = " + queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

}
