package org.nodel.discovery;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.nodel.diagnostics.AtomicLongMeasurementProvider;
import org.nodel.io.Stream;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.Timers;

/**
 * Holds shared resources for this package.
 */
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
     * The period between probes when actively probing.
     * (millis)
     */
    public static final int PROBE_PERIOD = 45000;    
    
    /**
     * How long to allow the multicast receiver to be silent. Will reinitialise socket as a precaution.
     */
    private static final int SILENCE_TOLERANCE = 3 * PROBE_PERIOD + 10000;    
    
    /**
     * (as an InetAddress; will never be null)
     */
    public static InetAddress _group = parseNumericalIPAddress(MDNS_GROUP);
    
    /**
     * (as an InetAddress; will never be null)
     */
    public static InetAddress _group_v6 = parseNumericalIPAddress(MDNS_GROUP_IPV6);
    
    /**
     * (as an InetSocketAddress (with port); will never be null)
     */
    public static InetSocketAddress _groupSocketAddress = new InetSocketAddress(_group, Discovery.MDNS_PORT);    
    
    /**
     * (instrumentation)
     */
    public static AtomicLong s_multicastOutOps = new AtomicLong();
    
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
    public static AtomicLong s_multicastOutData = new AtomicLong();
    
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
    public static AtomicLong s_multicastInOps = new AtomicLong();
    
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
    public static AtomicLong s_multicastInData = new AtomicLong();
    
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
    public static AtomicLong s_unicastOutOps = new AtomicLong();
    
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
    public static AtomicLong s_unicastOutData = new AtomicLong();
    
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
    public static AtomicLong s_unicastInOps = new AtomicLong();
    
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
    public static AtomicLong s_unicastInData = new AtomicLong();
    
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
     * This special method ('hack') returns the likely 'public' interface address. It's the most convenient way in Java
     * to interrogate the IP routing table when multiple network interfaces are present.
     * 
     * It uses a UDP socket to test the table without actually sending anything.
     * 
     * Should normally be called when a topology change is detected.
     */
    public static InetAddress getLikelyPublicAddress() {
        final String[] DEFAULT_TESTS = new String[] { "8.8.8.8",         // when a default gateway exists
                                                      "255.255.255.255", // when subnet routing exists
                                                      "127.0.0.1" };     // when only a loopback exists
        InetAddress result = null;
        
        try {
            for (String target : DEFAULT_TESTS) {
                DatagramSocket ds = null;
                try {
                    ds = new DatagramSocket();
                    
                    // this will not _actually_ connect to anything
                    ds.connect(new InetSocketAddress(InetAddress.getByName(target), 0));
                    
                    result = ds.getLocalAddress();
                    
                    break;
                    
                } catch (Exception exc) {
                    // this test failed, continue with the others
                    continue;
                    
                } finally {
                    Stream.safeClose(ds);
                }
            } // (for)

        } catch (Exception exc) {
            // all tests failed, just return the loop-back
        }
        
         return result != null ? result : Discovery.parseNumericalIPAddress("127.0.0.1");
    }

    /**
     * Parses a dotted numerical IP address without throwing any exceptions.
     * (convenience function)
     */
    private static InetAddress parseNumericalIPAddress(String numerical) {
        try {
            return InetAddress.getByName(numerical);

        } catch (Exception exc) {
            throw new Error("Failed to resolve numerical IPv4 or IPv6 address - " + numerical);
        }
    }

}
