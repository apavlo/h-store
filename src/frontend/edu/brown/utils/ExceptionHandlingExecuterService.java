package edu.brown.utils;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * @author Mike Herzog, 2009
 * http://stackoverflow.com/a/3535235/42171
 */
public class ExceptionHandlingExecuterService extends ScheduledThreadPoolExecutor {

    /** My ExceptionHandler */
    private final UncaughtExceptionHandler exceptionHandler;

    /**
     * Encapsulating a task and enable exception handling.
     * <p>
     * <i>NB:</i> We need this since {@link ExecutorService}s ignore the
     * {@link UncaughtExceptionHandler} of the {@link ThreadFactory}.
     * 
     * @param <V> The result type returned by this FutureTask's get method.
     */
    private class ExceptionHandlingFutureTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {

        /** Encapsulated Task */
        private final RunnableScheduledFuture<V> task;

        /**
         * Encapsulate a {@link Callable}.
         * 
         * @param callable
         * @param task
         */
        public ExceptionHandlingFutureTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
            super(callable);
            this.task = task;
        }

        /**
         * Encapsulate a {@link Runnable}.
         * 
         * @param runnable
         * @param result
         * @param task
         */
        public ExceptionHandlingFutureTask(Runnable runnable, RunnableScheduledFuture<V> task) {
            super(runnable, null);
            this.task = task;
        }

        /*
         * (non-Javadoc)
         * @see java.util.concurrent.FutureTask#done() The actual exception
         * handling magic.
         */
        @Override
        protected void done() {
            // super.done(); // does nothing
            try {
                get();

            } catch (ExecutionException e) {
                if (exceptionHandler != null) {
                    exceptionHandler.uncaughtException(null, e.getCause());
                }

            } catch (Exception e) {
                // never mind cancelation or interruption...
            }
        }

        @Override
        public boolean isPeriodic() {
            return this.task.isPeriodic();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return task.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed other) {
            return task.compareTo(other);
        }

    }

    /**
     * @param corePoolSize The number of threads to keep in the pool, even if
     *        they are idle.
     * @param eh Receiver for unhandled exceptions. <i>NB:</i> The thread
     *        reference will always be <code>null</code>.
     */
    public ExceptionHandlingExecuterService(int corePoolSize, ThreadFactory threadFactory, UncaughtExceptionHandler eh) {
        super(corePoolSize, threadFactory);
        this.exceptionHandler = eh;
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        return new ExceptionHandlingFutureTask<V>(callable, task);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        return new ExceptionHandlingFutureTask<V>(runnable, task);
    }
}
