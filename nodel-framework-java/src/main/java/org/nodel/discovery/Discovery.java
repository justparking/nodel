package org.nodel.discovery;

import java.net.InetAddress;

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
     * (as an InetAddress; will never be null)
     */
    public static InetAddress _group = parseNumericalIPAddress(MDNS_GROUP);
    
    /**
     * (as an InetAddress; will never be null)
     */
    public static InetAddress _group_v6 = parseNumericalIPAddress(MDNS_GROUP_IPV6);    

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
