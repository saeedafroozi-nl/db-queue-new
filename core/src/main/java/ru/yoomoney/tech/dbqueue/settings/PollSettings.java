package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * Task polling settings.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
public class PollSettings extends DynamicSetting<PollSettings> {
    @Nonnull
    private Duration betweenTaskTimeout;
    @Nonnull
    private Duration noTaskTimeout;
    @Nonnull
    private Duration fatalCrashTimeout;
    @Nonnull
    private Integer batchSize;
    @Nonnull
    private Integer queryVersion;

    private PollSettings(@Nonnull Duration betweenTaskTimeout,
                         @Nonnull Duration noTaskTimeout,
                         @Nonnull Duration fatalCrashTimeout,
                         @Nonnull Integer batchSize,
                         @Nonnull Integer queryVersion) {
        this.betweenTaskTimeout = requireNonNull(betweenTaskTimeout, "betweenTaskTimeout must not be null");
        this.noTaskTimeout = requireNonNull(noTaskTimeout, "noTaskTimeout must not be null");
        this.fatalCrashTimeout = requireNonNull(fatalCrashTimeout, "fatalCrashTimeout must not be null");
        this.batchSize = requireNonNull(batchSize, "batchSize must not be null");
        this.queryVersion =  requireNonNull(queryVersion, "queryVersion must not be null");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must not be less then 1");
        }
    }

    /**
     * Get delay duration between picking tasks from the queue after the task was processed.
     *
     * @return Delay after next task was processed.
     */
    @Nonnull
    public Duration getBetweenTaskTimeout() {
        return betweenTaskTimeout;
    }

    /**
     * Get delay duration between picking tasks from the queue if there are no task for processing.
     *
     * @return Delay when there are no tasks to process.
     */
    @Nonnull
    public Duration getNoTaskTimeout() {
        return noTaskTimeout;
    }

    /**
     * Get delay duration when task execution thread sleeps after unexpected error.
     *
     * @return Delay after unexpected error.
     */
    @Nonnull
    public Duration getFatalCrashTimeout() {
        return fatalCrashTimeout;
    }

    /**
     * Get size of batch picked from queue at one time
     *
     * @return Size of batch.
     */
    @Nonnull
    public Integer getBatchSize() {
        return batchSize;
    }

    @Deprecated(forRemoval = true)
    @Nonnull
    public Integer getQueryVersion() {
        return queryVersion;
    }

    /**
     * Create a new builder for poll settings.
     *
     * @return A new builder for poll settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PollSettings that = (PollSettings) obj;
        return betweenTaskTimeout.equals(that.betweenTaskTimeout) && noTaskTimeout.equals(that.noTaskTimeout)
                && fatalCrashTimeout.equals(that.fatalCrashTimeout)
                && batchSize.equals(that.batchSize)
                && queryVersion.equals(that.queryVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(betweenTaskTimeout, noTaskTimeout, fatalCrashTimeout, batchSize, queryVersion);
    }

    @Override
    public String toString() {
        return "{" +
                "betweenTaskTimeout=" + betweenTaskTimeout +
                ", noTaskTimeout=" + noTaskTimeout +
                ", fatalCrashTimeout=" + fatalCrashTimeout +
                ", batchSize=" + batchSize +
                ", queryVersion=" + queryVersion +
                '}';
    }

    @Nonnull
    @Override
    protected String getName() {
        return "pollSettings";
    }

    @Nonnull
    @Override
    protected BiFunction<PollSettings, PollSettings, String> getDiffEvaluator() {
        return (oldVal, newVal) -> {
            StringJoiner diff = new StringJoiner(",", getName() + '(', ")");
            if (!Objects.equals(oldVal.betweenTaskTimeout, newVal.betweenTaskTimeout)) {
                diff.add("betweenTaskTimeout=" +
                        newVal.betweenTaskTimeout + '<' + oldVal.betweenTaskTimeout);
            }
            if (!Objects.equals(oldVal.noTaskTimeout, newVal.noTaskTimeout)) {
                diff.add("noTaskTimeout=" +
                        newVal.noTaskTimeout + '<' + oldVal.noTaskTimeout);
            }
            if (!Objects.equals(oldVal.fatalCrashTimeout, newVal.fatalCrashTimeout)) {
                diff.add("fatalCrashTimeout=" +
                        newVal.fatalCrashTimeout + '<' + oldVal.fatalCrashTimeout);
            }
            if (!Objects.equals(oldVal.batchSize, newVal.batchSize)) {
                diff.add("batchSize=" +
                        newVal.batchSize + '<' + oldVal.batchSize);
            }
            if (!Objects.equals(oldVal.batchSize, newVal.batchSize)) {
                diff.add("queryVersion=" +
                        newVal.queryVersion + '<' + oldVal.queryVersion);
            }
            return diff.toString();
        };
    }

    @Nonnull
    @Override
    protected PollSettings getThis() {
        return this;
    }

    @Override
    protected void copyFields(@Nonnull PollSettings newValue) {
        this.betweenTaskTimeout = newValue.betweenTaskTimeout;
        this.noTaskTimeout = newValue.noTaskTimeout;
        this.fatalCrashTimeout = newValue.fatalCrashTimeout;
        this.batchSize = newValue.batchSize;
        this.queryVersion = newValue.queryVersion;
    }

    /**
     * A builder for poll settings.
     */
    public static class Builder {

        private Duration betweenTaskTimeout;
        private Duration noTaskTimeout;
        private Duration fatalCrashTimeout;
        private Integer batchSize;
        private Integer queryVersion = 0;

        private Builder() {
        }

        /**
         * Set delay duration between picking tasks from the queue
         * after the task was processed.
         *
         * @param betweenTaskTimeout Delay after next task was processed.
         * @return Reference to the same builder.
         */
        public Builder withBetweenTaskTimeout(@Nonnull Duration betweenTaskTimeout) {
            this.betweenTaskTimeout = betweenTaskTimeout;
            return this;
        }

        /**
         * Set delay duration between picking tasks from the queue
         * if there are no task for processing.
         *
         * @param noTaskTimeout Delay when there are no tasks to process.
         * @return Reference to the same builder.
         */
        public Builder withNoTaskTimeout(@Nonnull Duration noTaskTimeout) {
            this.noTaskTimeout = noTaskTimeout;
            return this;
        }

        /**
         * Set delay duration after unexpected error.
         *
         * @param fatalCrashTimeout Delay after unexpected error.
         * @return Reference to the same builder.
         */
        public Builder withFatalCrashTimeout(@Nonnull Duration fatalCrashTimeout) {
            this.fatalCrashTimeout = fatalCrashTimeout;
            return this;
        }

        /**
         * Set size of batch picked from queue at one time.
         *
         * @param batchSize Size of batch.
         * @return Reference to the same builder.
         */
        public Builder withBatchSize(@Nonnull Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder withQueryVersion(@Nullable Integer queryVersion) {
            if (queryVersion != null) {
                this.queryVersion = queryVersion;
            }
            return this;
        }

        /**
         * Create new poll settings object.
         *
         * @return A new poll settings object.
         */
        public PollSettings build() {
            return new PollSettings(betweenTaskTimeout, noTaskTimeout, fatalCrashTimeout, batchSize,
                    queryVersion);
        }
    }
}
