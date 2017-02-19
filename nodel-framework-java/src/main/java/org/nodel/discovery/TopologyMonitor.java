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

import org.nodel.Handler;
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
     * (for synchronization)
     */
    private Object _lock = new Object();

    /**
     * (synchronized)
     */
    private TimerTask _mainTimer;

    /**
     * The last active ones.
     */
    private Set<NetworkInterface> _lastActiveSet = new HashSet<NetworkInterface>();

    /**
     * (callbacks)
     */
    private Handler.H2<List<NetworkInterface>, List<NetworkInterface>> _onChangeHandler;

    /**
     * Adds a callback for topology changes (order is "new interfaces", "old interfaces")
     */
    public void setOnChangeHandler(Handler.H2<List<NetworkInterface>, List<NetworkInterface>> handler) {
        _onChangeHandler = handler;
    }

    /**
     * (see related 'add__Handler')
     */
    public void removeOnChangeHandler(Handler.H2<List<NetworkInterface>, List<NetworkInterface>> handler) {
        _onChangeHandler = null;
    }

    /**
     * After construction, use 'add___Handler()' and then start().
     */
    public TopologyMonitor() {
    }
    
    /**
     * Starts this topology monitor permanently.
     */
    public void start() {
        synchronized(_lock) {
            if (_mainTimer != null)
                throw new IllegalStateException("Already started");
            
            kickOffTimer();
        }
    }
    
    /**
     * kick off a monitor after 2 seconds, 1 minute, then every 4 minutes to cover a typical
     * fresh system boot-up sequence where interfaces may take some time to settle.
     */
    private void kickOffTimer() {
        synchronized (_lock) {
            _mainTimer = _timerThread.schedule(_threadPool, new TimerTask() {

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
        
        // update 'last active' with new and old
        for (NetworkInterface intf : newly)
            _lastActiveSet.add(intf);
        for (NetworkInterface intf : gone)
            _lastActiveSet.remove(intf);

        // do some logging
        if (newly.size() > 0)
            _logger.info("New interfaces discovered: " + newly);
        if (gone.size() > 0)
            _logger.info("Interfaces gone missing:" + gone);

        // notify of changes
        Handler.handle(_onChangeHandler, newly, gone);
    }
    
    /**
     * (exceptionless: even though method should be exception, provide this guarantee otherwise timer may fail.)
     */
    private void tryMonitorInterfaces() {
        try {
            monitorInterfaces();
            
        } catch (Exception exc) {
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
