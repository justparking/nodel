package org.nodel.threading;

import org.slf4j.*;

import java.util.*;

public class ThreadPool {

    /**
     * (logging)
     */
    protected final Logger _logger;

    private final String _name;

    /**
     * Capacity of thread-pool
     */
    private final int _capacity;

    /**
     * (locked around 'queue')
     */
    private int _busy = 0;

    /**
     * (main lock)
     */
    private final Queue<Runnable> _queue = new LinkedList<>();

    public ThreadPool(String name, int capacity) {
        _name = name;
        _logger = LoggerFactory.getLogger(String.format("%s.%s", this.getClass().getName(), name));
        _capacity = capacity;
    }


    /**
     * Applies a limited thread-pool capacity using an under-lying shared thread-pool
     */
    public void execute(final Runnable runnable) {
        // immediately indicate its busy

        synchronized (_queue) {
            _busy += 1;

            _queue.add(runnable);

            if (_busy > _capacity) {
                // active/busy capacity exceeded, leave queue to be dealt with by active threads
                return;
            }
        }

        // there is spare capacity, so execute on the thread-pool
        ThreadLake.background().execute(new Runnable() {

            @Override
            public void run() {
                for (; ; ) {
                    Runnable nextToRun;
                    synchronized (_queue) {
                        nextToRun = _queue.poll();

                        if (nextToRun == null) {
                            // all done, this thread not busy anymore
                            _busy -= 1;
                            return;
                        }
                    }

                    try {
                        nextToRun.run();

                    } catch (Exception exc) {
                        _logger.warn("An unhandled exception occurred within this isolated thread-pool", exc);
                    }

                    // continually loop while queue has items...
                }
            }

        });
    }

    /**
     * (class-level lock)
     */
    private final static Object s_lock = new Object();

    /**
     * Holds the back-ground thread-pool.
     */
    private static ThreadPool s_background;

    private final static int DEFAULT_MAXTHREADS = 128;

    /**
     * Background thread-pool for low-priority tasks.
     * (singleton)
     */
    public static ThreadPool background() {
        if (s_background == null) {
            synchronized(s_lock) {
                if (s_background == null)
                    s_background = new ThreadPool("Background", DEFAULT_MAXTHREADS);
            }
        }
        return s_background;
    }

}
