package org.nodel.discovery;

/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.nodel.DateTimes;
import org.nodel.Exceptions;
import org.nodel.Handler;
import org.nodel.SimpleName;
import org.nodel.core.NodeAddress;
import org.nodel.core.Nodel;
import org.nodel.io.Stream;
import org.nodel.io.UTF8Charset;
import org.nodel.reflection.Serialisation;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.TimerTask;
import org.nodel.threading.Timers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for registering / unregistering nodes for discovery / lookup purposes.
 */
public class NodelAutoDNS extends AutoDNS {

    /**
     * The period between probes when actively probing. (millis)
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
     * Used to micro-stagger responses.
     */
    private Random _random = new Random();

    /**
     * (convenience reference)
     */
    private ThreadPool _threadPool = Discovery.threadPool();

    /**
     * (convenience timers)
     */
    private Timers _timerThread = Discovery.timerThread();

    /**
     * (for graceful clean up)
     */
    private List<TimerTask> _timers = new ArrayList<TimerTask>();

    /**
     * (permanently latches false)
     */
    private volatile boolean _enabled = true;

    /**
     * (used in 'incomingQueue')
     */
    private class QueueEntry {

        /**
         * For logging purposes
         */
        public final PersistentBinder source;

        /**
         * The packet
         */
        public final DatagramPacket packet;

        public QueueEntry(PersistentBinder source, DatagramPacket packet) {
            this.source = source;
            this.packet = packet;
        }

    }

    /**
     * The incoming queue. (self locked)
     */
    private Queue<QueueEntry> _incomingQueue = new LinkedList<QueueEntry>();

    /**
     * Used to avoid unnecessary thread overlapping.
     */
    private boolean _isProcessingIncomingQueue = false;

    /**
     * Incoming queue processor runnable.
     */
    private Runnable _incomingQueueProcessor = new Runnable() {

        @Override
        public void run() {
            processIncomingPacketQueue();
        }

    };

    /**
     * Used in '_services'.
     */
    private class ServiceItem {

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
     * Whether or not we're probing for client. It will probe on start up and then deactivate. e.g. 'list' is called. (nanos)
     */
    private AtomicLong _lastList = new AtomicLong(System.nanoTime());

    /**
     * The last time a probe occurred. (nanos)
     */
    private AtomicLong _lastProbe = new AtomicLong(0);

    /**
     * Whether or not client resolution is being used. This affects whether polling should take place. (one way switch)
     */
    private volatile boolean _usingResolution = false;

    /**
     * The time before probing can be suspended if there haven't been any recent 'list' or 'resolve' operation. 5 minutes.
     * 
     * (millis)
     */
    private static final long LIST_ACTIVITY_PERIOD = 5 * 60 * 1000;

    /**
     * General purpose lock for sendSocket, receiveSocket, enabled, recycle*flag
     */
    private Object _lock = new Object();

    /**
     * Holds a safe list of resolved addresses and ports that should be "directly" multicast to (i.e. using unicast). Can be 
     * used if multicasting is unreliable or inconvenient. (is either null or has at least one element)
     */
    private List<InetSocketAddress> _hardLinksAddresses = composeHardLinksSocketAddresses();

    /**
     * Watches for changes to the network interface topology.
     */
    private TopologyMonitor _topologyMonitor;

    /**
     * The persistent interface binders. (synchronized around 'lock')
     */
    private Map<NetworkInterface, PersistentBinder> _activeInterfaces = new HashMap<NetworkInterface, PersistentBinder>();

    /**
     * A cached copy to the binder list for rapid enumeration.
     */
    private List<PersistentBinder> _bindersListCache = Collections.emptyList();

    /**
     * Returns immediately.
     * 
     * (Private constructor)
     */
    private NodelAutoDNS() {
        // no blocking code can be here

        _topologyMonitor = new TopologyMonitor();
        _topologyMonitor.setOnChangeHandler(new Handler.H2<List<NetworkInterface>, List<NetworkInterface>>() {

            @Override
            public void handle(List<NetworkInterface> appeared, List<NetworkInterface> disappeared) {
                handleTopologyChanges(appeared, disappeared);
            }

        });
        _topologyMonitor.start();

        // kick off the client prober to start
        // after 10s - 15s (randomly chosen)
        _timers.add(_timerThread.schedule(new TimerTask() {

            @Override
            public void run() {
                handleProbeTimer();
            }

        }, (long) (10000 + _random.nextDouble() * 5000), PROBE_PERIOD));

        // kick off the cleanup tasks timer
        _timers.add(_timerThread.schedule(new TimerTask() {

            @Override
            public void run() {
                handleCleanupTimer();
            }

        }, 60000, 60000));

        _logger.info("Auto discovery interface topology monitor and timers started. probePeriod:{}, stalePeriodAllowed:{}", DateTimes.formatShortDuration(PROBE_PERIOD), DateTimes.formatShortDuration(STALE_TIME));
    } // (init)

    /**
     * When interfaces appear and disappear.
     */
    private void handleTopologyChanges(List<NetworkInterface> appeared, List<NetworkInterface> disappeared) {
        // update the Nodel addresses immediately
        super.updateAddresses();
        
        synchronized (_lock) {
            // register, "wire up" and start those that have appeared
            for (NetworkInterface intf : appeared) {
                PersistentBinder binder = new PersistentBinder(intf);
                binder.setPacketHandler(new Handler.H2<PersistentBinder, DatagramPacket>() {

                    @Override
                    public void handle(PersistentBinder binder, DatagramPacket packet) {
                        handlePacketArrival(binder, packet);
                    }

                });
                binder.start();

                _activeInterfaces.put(intf, binder);
            }

            // unregister and shutdown those that have disappeared
            for (NetworkInterface intf : disappeared) {
                _activeInterfaces.remove(intf).shutdown();
            }

            // update the register to reflect new list of interfaces
            _bindersListCache = new ArrayList<PersistentBinder>(_activeInterfaces.values());
        }
    }

    /**
     * When a packet arrives from an interface
     */
    private void handlePacketArrival(PersistentBinder binder, DatagramPacket dp) {
        InetAddress recvAddr = dp.getAddress();

        if (recvAddr.isMulticastAddress()) {
            Discovery.s_multicastInData.addAndGet(dp.getLength());
            Discovery.s_multicastInOps.incrementAndGet();
        } else {
            Discovery.s_unicastInData.addAndGet(dp.getLength());
            Discovery.s_unicastInOps.incrementAndGet();
        }

        enqueueForProcessing(binder, dp);
    }

    private void enqueueForProcessing(PersistentBinder intf, DatagramPacket dp) {
        // place it in the queue and make it process if necessary
        synchronized (_incomingQueue) {
            QueueEntry qe = new QueueEntry(intf, dp);
            _incomingQueue.add(qe);

            // kick off the other thread to process the queue
            // (otherwise the thread will already be processing the queue)
            if (!_isProcessingIncomingQueue) {
                _isProcessingIncomingQueue = true;
                _threadPool.execute(_incomingQueueProcessor);
            }
        }
    }

    /**
     * Processes whatever's in the queue.
     */
    private void processIncomingPacketQueue() {
        while (_enabled) {
            QueueEntry entry;

            synchronized (_incomingQueue) {
                if (_incomingQueue.size() <= 0) {
                    // nothing left, so clear flag and return
                    _isProcessingIncomingQueue = false;

                    return;
                }

                entry = _incomingQueue.remove();
            }

            DatagramPacket dp = entry.packet;
            PersistentBinder source = entry.source;

            try {
                // parse packet
                NameServicesChannelMessage message = this.parsePacket(dp);

                // handle message
                this.handleIncomingMessage(source, (InetSocketAddress) dp.getSocketAddress(), message);

            } catch (Exception exc) {
                if (!_enabled)
                    break;

                // log nested exception summary instead of stack-trace dump
                _logger.warn("{} while handling received packet from {}: {}", source, dp.getSocketAddress(), Exceptions.formatExceptionGraph(exc));

            } finally {
                // make sure the packet is returned
                UDPPacketRecycleQueue.instance().returnPacket(entry.packet);
            }
        } // (while)
    }

    private boolean _suppressProbeLog = false;

    /**
     * The client timer; determines whether probing is actually necessary (timer entry-point)
     */
    private void handleProbeTimer() {
        if (_usingResolution) {
            // client names are being resolved, so stay probing
            _suppressProbeLog = false;
            sendProbe();

        } else {
            // the time difference in millis
            long listDiff = (System.nanoTime() - _lastList.get()) / 1000000L;

            if (listDiff < LIST_ACTIVITY_PERIOD) {
                _suppressProbeLog = false;
                sendProbe();

            } else {
                if (!_suppressProbeLog) {
                    _logger.info("Probing is paused because it has been more than {} since a 'list' or 'resolve' (total {}).", DateTimes.formatShortDuration(LIST_ACTIVITY_PERIOD), DateTimes.formatShortDuration(listDiff));
                }

                _suppressProbeLog = true;
            }
        }
    }

    /**
     * Performs the probe asynchronously.
     */
    private void sendProbe() {
        final NameServicesChannelMessage message = new NameServicesChannelMessage();

        message.agent = Nodel.getAgent();

        List<String> discoveryList = new ArrayList<String>(1);
        discoveryList.add("*");

        List<String> typesList = new ArrayList<String>(2);
        typesList.add("tcp");
        typesList.add("http");

        message.discovery = discoveryList;
        message.types = typesList;

        _lastProbe.set(System.nanoTime());

        // IO is involved so must use a thread-pool

        // Note: performing multiple sends on one thread here but UDP sending normally fails early and fast
        // so it's unlikely to cause any major blocking.
        _threadPool.execute(new Runnable() {

            @Override
            public void run() {
                // choose any available binder to send hardlinks message from
                PersistentBinder anyBinder = null;
                
                for (PersistentBinder binder : _bindersListCache) {
                    sendMessage(binder, Discovery._groupSocketAddress, message);

                    if (anyBinder == null)
                        anyBinder = binder;
                }

                // check if hard links (direct "multicasting") are enabled for some hosts
                if (_hardLinksAddresses != null && anyBinder != null) {
                    for (InetSocketAddress socketAddress : _hardLinksAddresses) {
                        sendMessage(anyBinder, socketAddress, message);
                    }
                }
            }

        });
    }

    /**
     * Handle clean-up tasks (timer entry-point)
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

                _logger.info("{} stale record{} removed. [{}]", toRemove.size(), toRemove.size() == 1 ? " was" : "s were", sb.toString());
            }
        }
    } // (method)

    /**
     * Parses the incoming packet.
     */
    private NameServicesChannelMessage parsePacket(DatagramPacket dp) {
        String packetString = new String(dp.getData(), 0, dp.getLength(), UTF8Charset.instance());

        return (NameServicesChannelMessage) Serialisation.coerceFromJSON(NameServicesChannelMessage.class, packetString);
    }

    /**
     * One responder per recipient at any stage.
     */

    private class Responder {

        public PersistentBinder _intf;

        public InetSocketAddress _recipient;

        public LinkedList<ServiceItem> _serviceSet;

        private Iterator<ServiceItem> _serviceIterator;

        public Responder(PersistentBinder intf, InetSocketAddress recipient) {
            _intf = intf;
            _recipient = recipient;

            synchronized (_serverLock) {
                // get a snap-shot of the service values
                _serviceSet = new LinkedList<ServiceItem>(_services.values());
            }

            // the service iterator
            _serviceIterator = _serviceSet.iterator();
        }

        /**
         * Starts the responder. Wait a random amount of time to before actually sending anything. Staggered responses assists with receiver buffer management by the remote receivers.
         */
        public void start(int randomResponseDelay) {
            // use a random minimum delay of 333ms
            int delay = 333;

            if (randomResponseDelay > 333)
                delay = randomResponseDelay;

            int timeToWait = _random.nextInt(delay);

            _timerThread.schedule(new TimerTask() {

                @Override
                public void run() {
                    completeResponse();
                }

            }, timeToWait);
        }

        /**
         * Complete the necessary response, also a timer entry-point.
         */
        private void completeResponse() {
            // prepare a message
            final NameServicesChannelMessage message = new NameServicesChannelMessage();
            message.present = new ArrayList<String>();

            // try keep the packets relatively small by roughly guessing how much space
            // its JSON advertisement packet might take up

            // account for "present"...
            // and "addresses":["tcp://136.154.27.100:65017","http://136.154.27.100:8085/index.htm?node=%NODE%"]
            // so start off with approx. 110 chars
            long roughTotalSize = 110;

            while (_serviceIterator.hasNext()) {
                ServiceItem si = _serviceIterator.next();

                String name = si._name.getOriginalName();

                // calculate size to be the name, two inverted-commas, and a comma and a possible space in between.
                int size = name.length() + 4;
                roughTotalSize += size;

                message.present.add(name);

                // make sure we're not going anywhere near UDP MTU (64K),
                // in fact, squeeze them into packets similar to size of Ethernet MTU (~1400 MTU)
                if (roughTotalSize > 1200)
                    break;
            } // (while)

            message.addresses = new ArrayList<String>();
            message.addresses.add(_nodelAddress);
            message.addresses.add(_httpAddress);
            
            // IO is involved so use thread-pool
            // (and only send if the 'present' list is not empty)

            if (message.present.size() > 0) {
                _threadPool.execute(new Runnable() {

                    @Override
                    public void run() {
                        sendMessage(_intf, _recipient, message);
                    }

                });
            }

            // do we need this completed in the very near future?
            // if so, space out by at least 333ms.
            if (_serviceIterator.hasNext()) {

                _timerThread.schedule(new TimerTask() {

                    @Override
                    public void run() {
                        completeResponse();
                    }

                }, 333);
            } else {
                synchronized (_responders) {
                    _responders.remove(_recipient);
                }
            }
        } // (method)

    } // (class)

    /**
     * Sends the message to a recipient
     */
    private void sendMessage(PersistentBinder intf, InetSocketAddress to, NameServicesChannelMessage message) {
        _logger.info("{}: sending message. to={}, message={}", intf, to, message);

        // convert into bytes
        String json = Serialisation.serialise(message);
        byte[] bytes = json.getBytes(UTF8Charset.instance());

        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        packet.setSocketAddress(to);

        try {
            intf.send(packet);

            if (to.getAddress().isMulticastAddress()) {
                Discovery.s_multicastOutData.addAndGet(bytes.length);
                Discovery.s_multicastOutOps.incrementAndGet();
            } else {
                Discovery.s_unicastOutData.addAndGet(bytes.length);
                Discovery.s_unicastOutOps.incrementAndGet();
            }

        } catch (Exception exc) {
            if (!_enabled)
                return;

            _logger.warn(intf + ": send() failed. ", exc);
        }
    } // (method)

    /**
     * The map of responders by recipient address.
     */
    private ConcurrentMap<SocketAddress, Responder> _responders = new ConcurrentHashMap<SocketAddress, Responder>();

    /**
     * Handles a complete packet from the socket.
     */
    private void handleIncomingMessage(PersistentBinder intf, InetSocketAddress from, NameServicesChannelMessage message) {
        _logger.info("{}: message arrived. from={}, message={}", intf, from, message);

        // discovery request?
        if (message.discovery != null) {
            // create a responder if one isn't already active

            if (_nodelAddress == null) {
                _logger.info("(will not respond; nodel server port still not available)");

                return;
            }

            synchronized (_responders) {
                if (!_responders.containsKey(from)) {
                    Responder responder = new Responder(intf, from);
                    _responders.put(from, responder);

                    int delay = message.delay == null ? 0 : message.delay.intValue();

                    responder.start(delay);
                }
            }
        }

        else if (message.present != null && message.addresses != null) {
            for (String name : message.present) {
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
        }

    } // (method)

    @Override
    public NodeAddress resolveNodeAddress(SimpleName node) {
        // indicate client resolution is being used
        _usingResolution = true;

        AdvertisementInfo adInfo = _advertisements.get(node);
        if (adInfo != null) {
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
        synchronized (_serverLock) {
            if (!_services.containsKey(node))
                throw new IllegalStateException(node + " is not advertised anyway.");

            _services.remove(node);
        }
    }

    @Override
    public Collection<AdvertisementInfo> list() {
        long now = System.nanoTime();

        _lastList.set(now);

        // check how long it has been since the last probe (millis)
        long timeSinceProbe = (now - _lastProbe.get()) / 1000000L;

        if (timeSinceProbe > LIST_ACTIVITY_PERIOD)
            sendProbe();

        // create snap-shot
        List<AdvertisementInfo> ads = new ArrayList<AdvertisementInfo>(_advertisements.size());

        synchronized (_clientLock) {
            ads.addAll(_advertisements.values());
        }

        return ads;
    }

    @Override
    public AdvertisementInfo resolve(SimpleName node) {
        // indicate client resolution is being used
        _usingResolution = true;

        return _advertisements.get(node);
    }

    /**
     * Permanently shuts down all related resources.
     */
    @Override
    public void close() throws IOException {
        // clear flag
        _enabled = false;

        // release timers
        for (TimerTask timer : _timers)
            timer.cancel();

        Stream.safeClose(_topologyMonitor);
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

    /**
     * Turns the list of addresses into "resolved" InetSocketAddresses. (should only be called once)
     */
    private List<InetSocketAddress> composeHardLinksSocketAddresses() {
        List<InetAddress> addresses = Nodel.getHardLinksAddresses();

        if (addresses.size() <= 0)
            return null;

        List<InetSocketAddress> socketAddresses = new ArrayList<InetSocketAddress>();
        for (InetAddress address : addresses)
            socketAddresses.add(new InetSocketAddress(address, Discovery.MDNS_PORT));

        return socketAddresses;
    }

} // (class)
