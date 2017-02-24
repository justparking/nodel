package org.nodel.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.nodel.DateTimes;
import org.nodel.Exceptions;
import org.nodel.Threads;
import org.nodel.core.Nodel;
import org.nodel.io.Stream;
import org.nodel.io.UTF8Charset;
import org.nodel.reflection.Serialisation;
import org.nodel.threading.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterfaceDiscoverer {

    /**
     * The period between probes when actively probing.
     * (millis)
     */
    private static final int PROBE_PERIOD = 45000;
    
    /**
     * (logging)
     */
    private Logger _logger = LoggerFactory.getLogger(NodelAutoDNS.class);
    
    /**
     * The thread to receive the multicast data.
     */
    private Thread _unicastHandlerThread;
    
    /**
     * (for graceful clean up)
     */
    private List<TimerTask> _timers = new ArrayList<TimerTask>();
    
    /**
     * (permanently latches false)
     */
    private volatile boolean _enabled = true;
    
    /**
     * (permanently latches false)
     */
    public boolean _started = false;

    /**
     * (used in 'incomingQueue')
     */
    private class QueueEntry {
        
        /**
         * The packet
         */
        public final DatagramPacket packet;
        
        public QueueEntry(DatagramPacket packet) {
            this.packet = packet;
        }
        
    }
    
    /**
     * The incoming queue.
     * (self locked)
     */
    private Queue<QueueEntry>_incomingQueue = new LinkedList<QueueEntry>();
    
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
     * Whether or not we're probing for client. It will probe on start up and then deactivate.
     * e.g. 'list' is called.
     * (nanos)
     */
    private AtomicLong _lastList = new AtomicLong(System.nanoTime());
    
    /**
     * The last time a probe occurred.
     * (nanos)
     */
    private AtomicLong _lastProbe = new AtomicLong(0);
    
    /**
     * Whether or not client resolution is being used. This affects whether polling
     * should take place. 
     * (one way switch)
     */
    private volatile boolean _usingResolution = false;
    
    /**
     * The time before probing can be suspended if there haven't been
     * any recent 'list' or 'resolve' operation. 5 minutes.
     * 
     * (millis)
     */
    private static final long LIST_ACTIVITY_PERIOD = 5 * 60 * 1000;
    
    /**
     * General purpose lock for sendSocket, receiveSocket, enabled, recycle*flag
     */
    private Object _lock = new Object();

    /**
     * For multicast sends and unicast receives on arbitrary port.
     * (locked around 'lock')
     */
    private MulticastSocket _sendSocket;
    
    private NodelAutoDNS _host;

    private InetAddress _intf;
    
    public InterfaceDiscoverer(InetAddress intf, NodelAutoDNS host) {
        _intf = intf;
        _host = host;
        
        // create the receiver thread and start it
        _unicastHandlerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                unicastReceiverThreadMain();
            }

        }, "autodns_unicastreceiver");
        _unicastHandlerThread.setDaemon(true);
        _unicastHandlerThread.start();
        
        // kick off the client prober to start
        // after 5s - 10s (randomly chosen)
        _timers.add(Discovery.timerThread().schedule(new TimerTask() {

            @Override
            public void run() {
                handleProbeTimer();
            }

        }, (long) (5000 + Discovery.random().nextDouble() * 5000), PROBE_PERIOD));

        _logger.info("Discoverer on this interface started. probePeriod:{}", DateTimes.formatShortDuration(PROBE_PERIOD));
    }


    /**
     * Creates a new socket, cleaning up if anything goes wrong in the process
     */
    private MulticastSocket createMulticastSocket(InetAddress intf, int port) throws Exception {
        MulticastSocket socket = null;

        try {
            _logger.info("Preparing socket. interface:{}, port:{}, group:{}", 
                    (intf == null ? "default" : intf), (port == 0 ? "any" : port), Discovery.MDNS_GROUP.getHostAddress());
            
            // in previous versions the interface was selected using constructor instead of 'socket.setInterface(intf)' 
            // but that uncovered side-effect in OSX which caused 'cannot assign address' Java bug
            
            socket = new MulticastSocket(port); // (port '0' means any port)
            
            if (intf != null)
                socket.setInterface(intf);

            // join the multicast group
            socket.joinGroup(Discovery.MDNS_GROUP);

            _logger.info("Ready. localAddr:{}", socket.getLocalSocketAddress());

            return socket;

        } catch (Exception exc) {
            Stream.safeClose(socket);

            throw exc;
        }
    }

    /**
     * (thread entry-point)
     */
    private void unicastReceiverThreadMain() {
        while (_enabled) {
            MulticastSocket socket = null;

            try {
                socket = createMulticastSocket(_intf, 0);

                while (_enabled) {
                    DatagramPacket dp = UDPPacketRecycleQueue.instance().getReadyToUsePacket();

                    try {
                        socket.receive(dp);

                    } catch (Exception exc) {
                        UDPPacketRecycleQueue.instance().returnPacket(dp);

                        throw exc;
                    }

                    if (dp.getAddress().isMulticastAddress()) {
                        Discovery.s_multicastInData.addAndGet(dp.getLength());
                        Discovery.s_multicastInOps.incrementAndGet();
                    } else {
                        Discovery.s_unicastInData.addAndGet(dp.getLength());
                        Discovery.s_unicastInOps.incrementAndGet();
                    }
                    
                    enqueueForProcessing(dp);

                } // (inner while)
                
            } catch (Exception exc) {
                boolean wasClosed = (socket != null && socket.isClosed());
                
                // clean up regardless
                Stream.safeClose(socket);

                synchronized (_lock) {
                    if (!_enabled)
                        break;

                    if (wasClosed)
                        _logger.info("Was signalled to gracefully close. Will reinitialise...");
                    else
                        _logger.warn("Receive failed; will reinitialise...", exc);

                    // stagger retry
                    Threads.waitOnSync(_lock, 333);
                }
            }
        } // (outer while)
        
        _logger.info("This thread has run to completion.");
    }

    private void enqueueForProcessing(DatagramPacket dp) {
        // place it in the queue and make it process if necessary
        synchronized (_incomingQueue) {
            QueueEntry qe = new QueueEntry(dp);
            _incomingQueue.add(qe);

            // kick off the other thread to process the queue
            // (otherwise the thread will already be processing the queue)
            if (!_isProcessingIncomingQueue) {
                _isProcessingIncomingQueue = true;
                Discovery.threadPool().execute(_incomingQueueProcessor);
            }
        }
    }    
    
    /**
     * Processes whatever's in the queue.
     */
    private void processIncomingPacketQueue() {
        while(_enabled) {
            QueueEntry entry;
            
            synchronized(_incomingQueue) {
                if (_incomingQueue.size() <= 0) {
                    // nothing left, so clear flag and return
                    _isProcessingIncomingQueue = false;

                    return;
                }

                entry = _incomingQueue.remove();
            }
            
            DatagramPacket dp = entry.packet;

            try {
                // parse packet
                NameServicesChannelMessage message = NameServicesChannelMessage.parsePacket(dp);

                // handle message
                this.handleIncomingMessage((InetSocketAddress) dp.getSocketAddress(), message);
                
            } catch (Exception exc) {
                if (!_enabled)
                    break;

                // log nested exception summary instead of stack-trace dump
                _logger.warn("While handling received packet from {}: {}", dp.getSocketAddress(), Exceptions.formatExceptionGraph(exc));
                
            } finally {
                // make sure the packet is returned
                UDPPacketRecycleQueue.instance().returnPacket(entry.packet);
            }
        } // (while) 
    }
    
    private boolean _suppressProbeLog = false;

    /**
     * The client timer; determines whether probing is actually necessary
     * (timer entry-point)
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
                    _logger.info("Probing is paused because it has been more than {} since a 'list' or 'resolve' (total {}).", 
                            DateTimes.formatShortDuration(LIST_ACTIVITY_PERIOD), DateTimes.formatShortDuration(listDiff));
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

        // IO is involved so use a thread-pool
        Discovery.threadPool().execute(new Runnable() {

            @Override
            public void run() {
                sendMessage(_sendSocket, Discovery.GROUP_SOCKET_ADDRESS, message);
            }

        });
    }
    
    /**
     * Sends the message to a recipient 
     */
    private void sendMessage(DatagramSocket socket, InetSocketAddress to, NameServicesChannelMessage message) {
        if (isSameSocketAddress(socket, to))
            _logger.info("Sending message. to=self, message={}", message);
        else
            _logger.info("Sending message. to={}, message={}", to, message);
        
        if (socket == null) {
            _logger.info("Socket not available yet; ignoring send request.");
            return;
        }
        
        // convert into bytes
        String json = Serialisation.serialise(message);
        byte[] bytes = json.getBytes(UTF8Charset.instance());
        
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        packet.setSocketAddress(to);
        
        try {
            socket.send(packet);
            
            if (to.getAddress().isMulticastAddress()) {
                Discovery.s_multicastOutData.addAndGet(bytes.length);
                Discovery.s_multicastOutOps.incrementAndGet();
            } else {
                Discovery.s_unicastOutData.addAndGet(bytes.length);
                Discovery.s_unicastOutOps.incrementAndGet();
            }

        } catch (IOException exc) {
            if (!_enabled)
                return;
            
            if (socket.isClosed())
                _logger.info("send() ignored as socket is being recycled.");
            else
                _logger.warn("send() failed. ", exc);
        }
    } // (method)
    
    /**
     * Handles a complete packet from the socket.
     */
    private void handleIncomingMessage(InetSocketAddress from, NameServicesChannelMessage message) {
        if (isSameSocketAddress(_sendSocket, from))
            _logger.info("Message arrived. from=self, message={}", message);
        else
            _logger.info("Message arrived. from={}, message={}", from, message);
        
        // discovery request?
        if (message.present != null && message.addresses != null) {
            for (String name : message.present ) {
                _host.updateAdvertisement(name, message);
            }
        }
        
    } // (method)
    
    public void indicateListRequested() {
        long now = System.nanoTime();
        
        _lastList.set(now);
        
        // check how long it has been since the last probe (millis)
        long timeSinceProbe = (now - _lastProbe.get()) / 1000000L;

        if (timeSinceProbe > LIST_ACTIVITY_PERIOD)
            sendProbe();
    }
    
    public void indicateUsingResolution() {
        // indicate client resolution is being used
        _usingResolution = true;
    }    

    /**
     * Permanently shuts down all related resources.
     */
    public void shutdown() {
        // clear flag
        _enabled = false;
        
        // release timers
        for (TimerTask timer : _timers)
            timer.cancel();

        Stream.safeClose(_sendSocket);
    }
    
    /**
     * Safely returns true if a packet has the same address and a socket. Used to determine its own socket.
     */
    private static boolean isSameSocketAddress(DatagramSocket socket, InetSocketAddress addr) {
        if (socket == null || addr == null)
            return false;
        
        SocketAddress socketAddr = socket.getLocalSocketAddress();
        
        return socketAddr != null && socketAddr.equals(addr);
    }
    
}
