package org.nodel.discovery;

import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.nodel.io.Stream;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.TimerTask;
import org.nodel.threading.Timers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks changes to the network interface topology e.g. networks appearing and disappearing.
 * 
 * Starts after a short delay and then periodically (every 4 mins)
 */
public class TopologyMonitor {

    /**
     * (logging)
     */
    private Logger _logger = LoggerFactory.getLogger(TopologyMonitor.class);
    
    /**
     * (convenience reference)
     */
    private ThreadPool _threadPool = Discovery.threadPool();
    
    /**
     * (convenience reference)
     */
    private Timers _timerThread = Discovery.timerThread();
    
    /**
     * The last active ones.
     */
    private Set<InetAddress> _lastActiveSet = new HashSet<InetAddress>();
    
    /**
     * (see addChangeHandler())
     */
    public static interface ChangeHandler {
        
        public void handle(List<InetAddress> appeared, List<InetAddress> disappeared);
        
    }

    /**
     * Newly registered callbacks. For thread safety, these move to 'onChangeHandlers' on first fire.
     * (synchronized)
     */
    private Set<ChangeHandler> _newOnChangeHandlers = new HashSet<ChangeHandler>();
    
    /**
     * Holds unregistered handlers
     * (synchronized)
     */
    private Set<ChangeHandler> _removedOnChangeHandlers = new HashSet<ChangeHandler>();
    
    /**
     * Active set callbacks
     * (only modified by one thread)
     */
    private List<ChangeHandler> _onChangeHandlers = new ArrayList<ChangeHandler>();

    private InetAddress _likelyPublicAddress = Discovery.IPv4Loopback;

    /**
     * Adds a callback for topology changes (order is "new interfaces", "old interfaces")
     */
    public void addOnChangeHandler(ChangeHandler handler) {
        synchronized (_newOnChangeHandlers) {
            _newOnChangeHandlers.add(handler);
        }
    }

    /**
     * (see related 'add__Handler')
     */
    public void removeOnChangeHandler(ChangeHandler handler) {
        synchronized (_removedOnChangeHandlers) {
            _removedOnChangeHandlers.add(handler);
        }
    }

    /**
     * (private constructor)
     */
    private TopologyMonitor() {
        kickOffTimer();
    }

    /**
     * (singleton, thread-safe, non-blocking)
     */
    private static class Instance {

        private static final TopologyMonitor INSTANCE = new TopologyMonitor();

    }

    /**
     * Returns the singleton instance of this class.
     */
    public static TopologyMonitor shared() {
        return Instance.INSTANCE;
    }
    
    /**
     * For adjustable polling intervals.
     */
    private int[] POLLING_INTERVALS = new int[] {2000, 5000, 15000, 15000, 60000, 120000};
    
    /**
     * (relates to POLLING_INTERVALS)
     */
    private int _pollingSlot = 0;
    
    /**
     * kick off a monitor after 2 seconds, then gradually up to every 4 minutes to cover a typical
     * fresh system boot-up sequence where interfaces may take some time to settle.
     */
    private void kickOffTimer() {
        _timerThread.schedule(_threadPool, new TimerTask() {

            @Override
            public void run() {
                // move the polling "slot" up
                _pollingSlot = Math.min(_pollingSlot + 1, POLLING_INTERVALS.length - 1);
               
                tryMonitorInterfaces();

                // then after a minute...
                _timerThread.schedule(_threadPool, this, POLLING_INTERVALS[_pollingSlot]);
            }

        }, 2000);
    }
    
    /**
     * Enumerates the interfaces and performs the actual before/after checks.
     * (involves blocking I/O)
     */
    private void monitorInterfaces() {
        Set<InetAddress> activeSet = new HashSet<>(4);

        listValidInterfaces(activeSet);

        // new interfaces
        List<InetAddress> newly = new ArrayList<>(activeSet.size());

        for (InetAddress newIntf : activeSet) {
            if (!_lastActiveSet.contains(newIntf))
                newly.add(newIntf);
        }

        // disappeared interfaces
        List<InetAddress> gone = new ArrayList<>(activeSet.size());

        for (InetAddress lastActiveIntf : _lastActiveSet) {
            if (!activeSet.contains(lastActiveIntf))
                gone.add(lastActiveIntf);
        }

        boolean hasChanged = false;

        // update 'last active' with new and old

        for (InetAddress intf : newly) {
            _lastActiveSet.add(intf);
            hasChanged = true;

            _logger.info("{}: interface appeared!", intf);
        }

        for (InetAddress intf : gone) {
            _lastActiveSet.remove(intf);
            hasChanged = true;

            _logger.info("{}: interface disappeared! ", intf);
        }
                
        // do internal test first before notifying anyone
        if (hasChanged) {
            _likelyPublicAddress = testForLikelyPublicAddress(activeSet);
            
            // and temporarily poll quicker again (might be general NIC activity)
            _pollingSlot = 0;
        }
 
        // now notify handlers...

        // remove previous handlers
        synchronized (_removedOnChangeHandlers) {
            if (_removedOnChangeHandlers.size() > 0) {
                for (ChangeHandler handler : _removedOnChangeHandlers)
                    _onChangeHandlers.remove(handler);
            }
        }

        // move any new handlers into this set
        List<ChangeHandler> newHandlers = null;

        synchronized (_newOnChangeHandlers) {
            if (_newOnChangeHandlers.size() > 0) {
                newHandlers = new ArrayList<ChangeHandler>(_newOnChangeHandlers);

                // no longer new now...
                _newOnChangeHandlers.clear();
            }
        }

        // notify recently new handlers first of the full active set so they can synchronise their lists
        if (newHandlers != null) {
            // 'lastActiveSet' is only modified by one thread so thread-safe
            List<InetAddress> activeList = new ArrayList<>(_lastActiveSet);

            for (ChangeHandler handler : newHandlers)
                handler.handle(activeList, Collections.<InetAddress>emptyList());
        }

        // notify existing handlers of changes
        if (hasChanged) {
            List<ChangeHandler> cachedList;

            cachedList = new ArrayList<>(_onChangeHandlers);

            for (ChangeHandler handler : cachedList)
                handler.handle(newly, gone);
        }

        // move 'new' to existing
        if (newHandlers != null) {
            for (ChangeHandler handler : newHandlers)
                _onChangeHandlers.add(handler);
        }
    }

    /**
     * (exceptionless)
     */
    private void tryMonitorInterfaces() {
        try {
            monitorInterfaces();
            
        } catch (Exception exc) {
            // still 'catch' to provide guarantee otherwise timer could fail            
            _logger.warn("An exception should not occur within method.", exc);
        }
    }
    
    /**
     * Scans for 'up', non-loopback, multicast-supporting, network interfaces with at least one IPv4 address.
     */
    private void listValidInterfaces(Set<InetAddress> refNicSet) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            for (NetworkInterface intf : Collections.list(interfaces)) {
                try {
                    if (!intf.supportsMulticast() || intf.isLoopback() || !intf.isUp())
                        continue;

                    // check for at least one IPv4 address and check loopback status again for good measure
                    for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                        if (address instanceof Inet4Address)
                            refNicSet.add(address);
                    }

                } catch (Exception exc) {
                    // skip this interface
                }
            }
        } catch (Exception exc) {
            warn("intf_enumeration", "Was not able to enumerate network interfaces", exc);
        }
    }
    
    /**
     * Returns the likely public address given the detected topology.
     * (returns immediately)
     */
    public InetAddress getLikelyPublicAddress() {
        return _likelyPublicAddress;
    }

    
    /**
     * This special method ('hack') returns the likely 'public' interface address. It's the most convenient way in Java
     * to interrogate the IP routing table when multiple network interfaces are present.
     * 
     * It uses a UDP socket to test the table. Unfortunately, OSX requires that a packet
     * is actually sent before the interface can be determined. All other OSs do not.
     * 
     * Should be called when a topology change is detected.
     */
    private static InetAddress testForLikelyPublicAddress(Set<InetAddress> nics) {
        final String[] DEFAULT_TESTS = new String[] { "8.8.8.8",         // when a default gateway exists
                                                      "255.255.255.255", // when subnet routing exists
                                                      "127.0.0.1" };     // when only a loopback exists
        InetAddress result = null;

        // go through each target and test against each interface
        for (String target : DEFAULT_TESTS) {
            for (InetAddress intfAddr : nics) {
                DatagramSocket ds = null;
                try {
                    ds = new DatagramSocket(new InetSocketAddress(intfAddr, 0));
                    ds.connect(new InetSocketAddress(target, 88)); // (port 88 is arbitrary)

                    return intfAddr;

                } catch (Exception exc) {
                    // keep trying;

                } finally {
                    Stream.safeClose(ds);
                }
            }
        }

        return result != null ? result : Discovery.IPv4Loopback;
    } // (method)
    
    /**
     * Warning with suppression.
     */
    private void warn(String category, String msg, Exception exc) {
        _logger.warn(msg, exc);
    }

}
