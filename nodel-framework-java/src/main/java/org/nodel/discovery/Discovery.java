package org.nodel.discovery;

import org.nodel.threading.ThreadPool;
import org.nodel.threading.Timers;

public class Discovery {

    /**
     * (see public method)
     */
    private Timers _timerThread = new Timers("Discovery");

    /**
     * This package's timer-thread.
     */
    public static Timers timerThread() {
        return instance()._timerThread;
    }

    /**
     * (see public method)
     */
    private ThreadPool _threadPool = new ThreadPool("Discovery", 24);

    /**
     * This package's shared thread-pool
     */
    public static ThreadPool threadPool() {
        return instance()._threadPool;
    }


    /**
     * (private constructor)
     */
    private Discovery() {
    }

    /**
     * (singleton, thread-safe, non-blocking)
     */
    private static class Instance {

        private static final Discovery INSTANCE = new Discovery();

    }

    /**
     * Returns the singleton instance of this class.
     */
    private static Discovery instance() {
        return Instance.INSTANCE;
    }

}
