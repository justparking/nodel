package org.nodel.threading;

import org.nodel.diagnostics.Diagnostics;
import org.nodel.diagnostics.SharableMeasurementProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class ThreadPoolFence implements ThreadPool {

    private final Logger _logger = LoggerFactory.getLogger(this.getClass().getName());

    private final int _limit;

    private final GrowingThreadPool _threadPool;

    private final String _name;

    /**
     * The number of threads actually in use.
     * (init in const.)
     */
    private final SharableMeasurementProvider _threadsInUse;

    /**
     * Number of operations completed (stats)
     * (init in const.)
     */
    private final SharableMeasurementProvider _operations;

    /**
     * (locked around 'queue')
     */
    private int _busy = 0;

    private final Queue<Runnable> _queue = new LinkedList<>();

    public ThreadPoolFence(String name, int limit, GrowingThreadPool threadPool) {
        _name = name;
        _limit = limit;
        _threadPool = threadPool;

        _operations =  Diagnostics.shared().registerSharableCounter(_name + ".Fenced ops", true);
        _threadsInUse = Diagnostics.shared().registerSharableCounter(_name + ".Fenced active threads", false);
    }

    public void execute(Runnable runnable) {
        synchronized (_queue) {
            if (_busy < _limit) {
                // not every thread is busy
                _busy += 1;
                _threadPool.execute(new Runnable() {

                    @Override
                    public void run() {
                        // record that it's in use
                        _threadsInUse.incr();

                        Runnable current = runnable;
                        while (current != null) {
                            // count the operation *before* actual execution
                            _operations.incr();

                            try {
                                current.run();
                            } catch (Throwable th) {
                                _logger.warn("An unhandled exception occurred within a thread-pool", th);
                            }

                            // finished, check immediately if can service some more
                            synchronized (_queue) {
                                current = _queue.poll();
                                if (current == null) {
                                    _busy -= 1;
                                    break;
                                }
                            }
                        } // while

                        // record it's not in use
                        _threadsInUse.decr();
                    } // method

                });
            } else {
                // over the concurrency limit, so just add to the queue
                _queue.add(runnable);
            }
        } // sync
    } // method

}
