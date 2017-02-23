package org.nodel.discovery;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.nodel.diagnostics.AtomicLongMeasurementProvider;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.Timers;

public class Discovery {
    
    /**
     * IPv4 multicast group
     */
    public static final String MDNS_GROUP = "224.0.0.252";

    /**
     * IPv6 multicast group (not used here but reserved)
     */
    public static final String MDNS_GROUP_IPV6 = "FF02::FB";
    
    /**
     * Multicast port
     */
    public static final int MDNS_PORT = 5354;
    
    /**
     * (convenience)
     */
    public static InetAddress IPv4Loopback = parseNumericalIPAddress("127.0.0.1");

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
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_multicastOutOps = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_multicastOutOpsMeasurement = new AtomicLongMeasurementProvider(s_multicastOutOps);
    
    /**
     * Multicast in operations.
     */
    public static AtomicLongMeasurementProvider MulticastOutOpsMeasurement() {
        return s_multicastOutOpsMeasurement;
    }    
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_multicastOutData = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_multicastOutDataMeasurement = new AtomicLongMeasurementProvider(s_multicastOutData);
    
    /**
     * Multicast out data.
     */
    public static AtomicLongMeasurementProvider MulticastOutDataMeasurement() {
        return s_multicastOutDataMeasurement;
    }    
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_multicastInOps = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_multicastInOpsMeasurement = new AtomicLongMeasurementProvider(s_multicastInOps);
    
    /**
     * Multicast in operations.
     */
    public static AtomicLongMeasurementProvider MulticastInOpsMeasurement() {
        return s_multicastInOpsMeasurement;
    }
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_multicastInData = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_multicastInDataMeasurement = new AtomicLongMeasurementProvider(s_multicastInData);
    
    /**
     * Multicast in data.
     */
    public static AtomicLongMeasurementProvider MulticastInDataMeasurement() {
        return s_multicastInDataMeasurement;
    }
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_unicastOutOps = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_unicastOutOpsMeasurement = new AtomicLongMeasurementProvider(s_unicastOutOps);
    
    /**
     * Unicast in operations.
     */
    public static AtomicLongMeasurementProvider UnicastOutOpsMeasurement() {
        return s_unicastOutOpsMeasurement;
    }    
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_unicastOutData = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_unicastOutDataMeasurement = new AtomicLongMeasurementProvider(s_unicastOutData);
    
    /**
     * Unicast out data.
     */
    public static AtomicLongMeasurementProvider UnicastOutDataMeasurement() {
        return s_unicastOutDataMeasurement;
    }    
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_unicastInOps = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_unicastInOpsMeasurement = new AtomicLongMeasurementProvider(s_unicastInOps);
    
    /**
     * Unicast in operations.
     */
    public static AtomicLongMeasurementProvider UnicastInOpsMeasurement() {
        return s_unicastInOpsMeasurement;
    }
    
    /**
     * (instrumentation)
     */
    static AtomicLong s_unicastInData = new AtomicLong();
    
    /**
     * (instrumentation)
     */
    private static AtomicLongMeasurementProvider s_unicastInDataMeasurement = new AtomicLongMeasurementProvider(s_unicastInData);

    /**
     * Unicast in data.
     */
    public static AtomicLongMeasurementProvider UnicastInDataMeasurement() {
        return s_unicastInDataMeasurement;
    }
    
    
    /**
     * Parses a dotted numerical IP address without throwing any exceptions.
     * (convenience function)
     */
    public static InetAddress parseNumericalIPAddress(String dottedNumerical) {
        try {
            return InetAddress.getByName(dottedNumerical);
            
        } catch (Exception exc) {
            throw new Error("Failed to resolve dotted numerical address - " + dottedNumerical);
        }
    }

}
