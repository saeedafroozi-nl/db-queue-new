package ru.yoomoney.tech.dbqueue.internal.processing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Вспомогательный класс, для задания стратегии
 * выполнения задачи в цикле
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
public interface QueueLoop {

    /**
     * Запустить выполнение кода
     *
     * @param runnable код для исполнения
     */
    void doRun(Runnable runnable);

    /**
     * Продолжить исполнение кода
     */
    void doContinue();

    /**
     * Приостановить исполнение кода
     *
     * @param timeout       промежуток на который следует приостановить работу
     * @param waitInterrupt признак, что разрешено прервать ожидание и продолжить работу
     */
    void doWait(Duration timeout, WaitInterrupt waitInterrupt);

    /**
     * Получить признак, что исполнение кода приостановлено
     *
     * @return true, если исполнение приостановлено
     */
    boolean isPaused();

    /**
     * Безусловно приостановить исполнение кода
     */
    void pause();

    /**
     * Безусловно продолжить исполнение кода
     */
    void unpause();

    /**
     * Cтратегия выполнения задачи в потоке
     */

    class WakeupQueueLoop implements QueueLoop {
        private static final Logger log = LoggerFactory.getLogger(WakeupQueueLoop.class);
        private final MillisTimeProvider millisTimeProvider = new MillisTimeProvider.SystemMillisTimeProvider();
        private final Object monitor = new Object();
        private volatile boolean isWakedUp = false;
        private volatile boolean isPaused = true;

        @Override
        public void doRun(Runnable runnable) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    synchronized (monitor) {
                        while (isPaused) {
                            monitor.wait();
                        }
                    }
                    runnable.run();
                } catch (InterruptedException ignored) {
                    log.info("sleep interrupted: threadName={}", Thread.currentThread().getName());
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void doContinue() {
            synchronized (monitor) {
                isWakedUp = true;
                monitor.notifyAll();
            }
        }

        @Override
        public void doWait(Duration timeout, WaitInterrupt waitInterrupt) {
            try {
                synchronized (monitor) {
                    long plannedWakeupTime = millisTimeProvider.getMillis() + timeout.toMillis();
                    long timeToSleep = plannedWakeupTime - millisTimeProvider.getMillis();
                    while (timeToSleep > 1L) {
                        if (!isWakedUp) {
                            monitor.wait(timeToSleep);
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.ALLOW) {
                            break;
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.DENY) {
                            isWakedUp = false;
                        }
                        timeToSleep = plannedWakeupTime - millisTimeProvider.getMillis();
                    }
                    isWakedUp = false;
                }
            } catch (InterruptedException ignored) {
                log.info("sleep interrupted: threadName={}", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean isPaused() {
            return isPaused;
        }

        @Override
        public void pause() {
            isPaused = true;
        }

        @Override
        public void unpause() {
            synchronized (monitor) {
                isPaused = false;
                monitor.notifyAll();
            }
        }
    }

    /**
     * Признак прерывания ожидания
     */
    enum WaitInterrupt {
        /**
         * Прерывание разрешено
         */
        ALLOW,
        /**
         * Прерывание запрещено
         */
        DENY
    }
}
