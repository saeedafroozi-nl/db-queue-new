package ru.yoomoney.tech.dbqueue.config.impl;

import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Empty listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public class NoopThreadLifecycleListener implements ThreadLifecycleListener {

    private static final NoopThreadLifecycleListener INSTANCE = new NoopThreadLifecycleListener();

    @Nonnull
    public static NoopThreadLifecycleListener getInstance() {
        return INSTANCE;
    }

}
