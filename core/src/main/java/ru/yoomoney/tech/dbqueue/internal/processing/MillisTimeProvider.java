package ru.yoomoney.tech.dbqueue.internal.processing;

import java.util.concurrent.TimeUnit;

/**
 * Поставщик текущего времени в миллисекундах.
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
@FunctionalInterface
public interface MillisTimeProvider {

    /**
     * Получить время в миллисекундах.
     *
     * @return время в миллисекундах
     */
    long getMillis();

    /**
     * Поставщик системного времени
     */
    class SystemMillisTimeProvider implements MillisTimeProvider {

        @Override
        public long getMillis() {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        }
    }
}
