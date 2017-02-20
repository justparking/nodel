package org.nodel.discovery;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private Set<NetworkInterface> _lastActiveSet = new HashSet<NetworkInterface>();
    
    /**
     * (see addChangeHandler())
     */
    public static interface ChangeHandler {
        
        public void handle(List<NetworkInterface> appeared, List<NetworkInterface> disappeared);
        
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
     * kick off a monitor after 2 seconds, 1 minute, then every 4 minutes to cover a typical
     * fresh system boot-up sequence where interfaces may take some time to settle.
     */
    private void kickOffTimer() {
        _timerThread.schedule(_threadPool, new TimerTask() {

            @Override
            public void run() {
                tryMonitorInterfaces();

                // then after a minute...
                _timerThread.schedule(_threadPool, new TimerTask() {

                    @Override
                    public void run() {
                        tryMonitorInterfaces();

                        // ...then every 4 minutes
                        _timerThread.schedule(_threadPool, this, 15000); // TODO: revert this
                    }

                }, 15000); // TODO: revert this to 60000
            }

        }, 2000);
    }
    
    /**
     * Enumerates the interfaces and performs the actual before/after checks.
     * (involves blocking I/O)
     */
    private void monitorInterfaces() {
        Set<NetworkInterface> activeSet = listValidInterfaces();

        // new interfaces
        List<NetworkInterface> newly = new ArrayList<NetworkInterface>(activeSet.size());

        for (NetworkInterface newIntf : activeSet) {
            if (!_lastActiveSet.contains(newIntf))
                newly.add(newIntf);
        }

        // disappeared interfaces
        List<NetworkInterface> gone = new ArrayList<NetworkInterface>(activeSet.size());

        for (NetworkInterface lastActiveIntf : _lastActiveSet) {
            if (!activeSet.contains(lastActiveIntf))
                gone.add(lastActiveIntf);
        }

        boolean hasChanged = false;

        // update 'last active' with new and old

        for (NetworkInterface intf : newly) {
            _lastActiveSet.add(intf);
            hasChanged = true;

            _logger.info("{}: interface appeared!", intf);
        }

        for (NetworkInterface intf : gone) {
            _lastActiveSet.remove(intf);
            hasChanged = true;

            _logger.info("{}: interface disappeared! ", intf);
        }

        // no notify handlers...

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
            List<NetworkInterface> activeList = new ArrayList<>(_lastActiveSet);

            for (ChangeHandler handler : newHandlers)
                handler.handle(activeList, Collections.<NetworkInterface>emptyList());
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
    private Set<NetworkInterface> listValidInterfaces() {
        Set<NetworkInterface> set = new HashSet<NetworkInterface>(3);
        
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            for (NetworkInterface intf : Collections.list(interfaces)) {
                try {
                    if (!intf.supportsMulticast() || intf.isLoopback() || !intf.isUp())
                        continue;

                    boolean valid = false;
                    
                    // check for at least one IPv4 address and check loopback status again for good measure
                    for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                        if (address instanceof Inet4Address)
                            valid = true;
                    }

                    // add to list if valid
                    if (valid)
                        set.add(intf);

                } catch (Exception exc) {
                    // skip this interface
                }
            }
        } catch (Exception exc) {
            warn("intf_enumeration", "Was not able to enumerate network interfaces", exc);
        }
        
        return set;
    }
    
    /**
     * Warning with suppression.
     */
    private void warn(String category, String msg, Exception exc) {
        _logger.warn(msg, exc);
    }

}
