package org.nodel.discovery;

/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.nodel.SimpleName;
import org.nodel.core.NodeAddress;
import org.nodel.threading.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for registering / unregistering nodes for discovery / lookup purposes. 
 */
public class NodelAutoDNS extends AutoDNS {
    
    /**
     * The period between probes when actively probing.
     * (millis)
     */
    private static final int PROBE_PERIOD = 45000;
    
    /**
	 * Expiry time (allow for at least one missing probe response)
	 */
	private static final long STALE_TIME = 2 * PROBE_PERIOD + 10000;
	
    /**
     * (logging)
     */
    private Logger _logger = LoggerFactory.getLogger(NodelAutoDNS.class);
    
    /**
     * General purpose lock / signal.
     */
    private Object _serverLock = new Object();

    /**
     * General purpose lock / signal.
     */    
    private Object _clientLock = new Object();
    
    /**
     * (for graceful clean up)
     */
    private TimerTask _cleanupTimer;
    
    /**
     * (permanently latches false)
     */
    private boolean _enabled = true;
    
    /**
     * Used in '_services'. 
     */
    public class ServiceItem {
        
        SimpleName _name;
        
        public ServiceItem(SimpleName name) {
            _name = name;
        }
        
    } // (class)
    
    /**
     * Holds the registered service items.
     */
    private ConcurrentMap<SimpleName, ServiceItem> _services = new ConcurrentHashMap<SimpleName, ServiceItem>();
    
    /**
     * Holds the collected advertisements.
     */
    private ConcurrentMap<SimpleName, AdvertisementInfo> _advertisements = new ConcurrentHashMap<SimpleName, AdvertisementInfo>();

    /**
     * General purpose lock for sendSocket, receiveSocket, enabled, recycle*flag
     */
    private Object _lock = new Object();
    
    private Map<InetAddress, InterfaceAdvertiser> _advertisers = new HashMap<>();
    
    private Map<InetAddress, InterfaceDiscoverer> _discoverers = new HashMap<>();

    /**
     * Returns immediately.
     * 
     * (Private constructor)
     */
    private NodelAutoDNS() {
        // (no blocking code can be here)
        TopologyMonitor.shared().addOnChangeHandler(new TopologyMonitor.ChangeHandler() {

            @Override
            public void handle(List<InetAddress> appeared, List<InetAddress> disappeared) {
                onTopologyChanged(appeared, disappeared);
            }

        });
        
        // kick off the cleanup tasks timer
        _cleanupTimer = Discovery.timerThread().schedule(new TimerTask() {

            @Override
            public void run() {
                handleCleanupTimer();
            }

        }, 60000, 60000);
    }

    /**
     * When the topology changes, reset the sockets to pick up the new public interface address
     */
    private void onTopologyChanged(List<InetAddress> appeared, List<InetAddress> disappeared) {
        synchronized(_lock) {
            for (InetAddress intf : disappeared) {
                _advertisers.remove(intf).shutdown();
                _discoverers.remove(intf).shutdown();
            }
            
            for (InetAddress intf: appeared) {
                _advertisers.put(intf, new InterfaceAdvertiser(intf, this));
                _discoverers.put(intf, new InterfaceDiscoverer(intf, this));
            }
        }
    }
    
	/**
     * Handle clean-up tasks
     * (timer entry-point)
     */
    private void handleCleanupTimer() {
        long currentTime = System.nanoTime();

        reapStaleRecords(currentTime);
    } // (method)  
    
    /**
     * Checks for stale records and removes them.
     */
    private void reapStaleRecords(long currentTime) {
        LinkedList<AdvertisementInfo> toRemove = new LinkedList<AdvertisementInfo>();

        synchronized (_clientLock) {
            for (AdvertisementInfo adInfo : _advertisements.values()) {
                long timeDiff = (currentTime / 1000000) - adInfo.timeStamp;

                if (timeDiff > STALE_TIME)
                    toRemove.add(adInfo);
            }

            // reap if necessary
            if (toRemove.size() > 0) {
                StringBuilder sb = new StringBuilder();

                for (AdvertisementInfo adInfo : toRemove) {
                    if (sb.length() > 0)
                        sb.append(",");

                    _advertisements.remove(adInfo.name);
                    sb.append(adInfo.name);
                }

                _logger.info("{} stale record{} removed. [{}]", 
                        toRemove.size(), toRemove.size() == 1 ? " was" : "s were", sb.toString());
            }
        }
    } // (method)

    @Override
    public NodeAddress resolveNodeAddress(SimpleName node) {
    	// indicate client resolution is being used
        synchronized(_discoverers) {
            for (InterfaceDiscoverer discoverer : _discoverers.values())
                discoverer.indicateUsingResolution();
        }
    	
        AdvertisementInfo adInfo = _advertisements.get(node);
        if(adInfo != null) {
            Collection<String> addresses = adInfo.addresses;
            
            for (String address : addresses) {
                try {
                    if (address == null || !address.startsWith("tcp://"))
                        continue;

                    int indexOfPort = address.lastIndexOf(':');
                    if (indexOfPort < 0 || indexOfPort >= address.length() - 2)
                        continue;

                    String addressPart = address.substring(6, indexOfPort);

                    String portStr = address.substring(indexOfPort + 1);

                    int port = Integer.parseInt(portStr);

                    NodeAddress nodeAddress = NodeAddress.create(addressPart, port);

                    return nodeAddress;

                } catch (Exception exc) {
                    _logger.info("'{}' node resolved to a bad address - '{}'; ignoring.", node, address);

                    return null;
                }
            }
        }
        
        return null;
    }

    @Override
    public void registerService(SimpleName node) {
        synchronized (_serverLock) {
            if (_services.containsKey(node))
                throw new IllegalStateException(node + " is already being advertised.");

            ServiceItem si = new ServiceItem(node);

            _services.put(node, si);
        }
    } // (method)

    @Override
    public void unregisterService(SimpleName node) {
        synchronized(_serverLock) {
            if (!_services.containsKey(node))
                throw new IllegalStateException(node + " is not advertised anyway.");

            _services.remove(node);
        }        
    }    

    @Override
    public Collection<AdvertisementInfo> list() {
    	synchronized(_discoverers) {
    	    for(InterfaceDiscoverer discoverer : _discoverers.values())
    	        discoverer.indicateListRequested();
    	}

        // create snap-shot
        List<AdvertisementInfo> ads = new ArrayList<AdvertisementInfo>(_advertisements.size());
        
        synchronized (_clientLock) {
            ads.addAll(_advertisements.values());
        }
        
        return ads;
    }
    
    public Collection<ServiceItem> getServicesSnapshot() {
        synchronized (_serverLock) {
            // get a snap-shot of the service values
            return new LinkedList<ServiceItem>(_services.values());
        }
    }
    
    
    public void updateAdvertisement(String name, NameServicesChannelMessage message) {
        synchronized (_clientLock) {
            SimpleName node = new SimpleName(name);
            AdvertisementInfo ad = _advertisements.get(node);
            if (ad == null) {
                ad = new AdvertisementInfo();
                ad.name = node;
                _advertisements.put(node, ad);
            }
            
            // refresh the time stamp and update the address
            ad.timeStamp = System.nanoTime() / 1000000;
            ad.addresses = message.addresses;
        }
    }
    
    
    @Override
    public AdvertisementInfo resolve(SimpleName node) {
        // indicate client resolution is being used
        synchronized(_discoverers) {
            for (InterfaceDiscoverer discoverer : _discoverers.values())
                discoverer.indicateUsingResolution();
        }
        
        return _advertisements.get(node);
    }

    /**
     * Permanently shuts down all related resources.
     */
    @Override
    public void close() throws IOException {
        // clear flag
        _enabled = false;
        
        synchronized (_advertisers) {
            for (InetAddress intf : new ArrayList<>(_advertisers.keySet()))
                _advertisers.remove(intf).shutdown();
        }
        
        synchronized (_discoverers) {
            for (InetAddress intf : new ArrayList<>(_discoverers.keySet()))
                _discoverers.remove(intf).shutdown();
        }
        
        // release timer
        _cleanupTimer.cancel();
    }
    
    /**
     * Creates or returns the shared instance.
     */
    public static AutoDNS create() {
        return Instance.INSTANCE;
    }
    
    /**
     * (singleton, thread-safe, non-blocking)
     */
    private static class Instance {
        
        private static final NodelAutoDNS INSTANCE = new NodelAutoDNS();
        
    }
    
    /**
     * Returns the singleton instance of this class.
     */
    public static NodelAutoDNS instance() {
        return Instance.INSTANCE;
    }

} // (class)
