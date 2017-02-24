package org.nodel.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.nodel.Exceptions;
import org.nodel.Threads;
import org.nodel.core.Nodel;
import org.nodel.discovery.NodelAutoDNS.ServiceItem;
import org.nodel.io.Stream;
import org.nodel.io.UTF8Charset;
import org.nodel.reflection.Serialisation;
import org.nodel.threading.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterfaceAdvertiser {
    
    /**
     * (init in constructor)
     */
    private Logger _logger;
    
    /**
     * The thread to receive the multicast data.
     */
    private Thread _multicastHandlerThread;
    
    /**
     * (permanently latches false)
     */
    private volatile boolean _enabled = true;
    
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
     * General purpose lock for sendSocket, receiveSocket, enabled, recycle*flag
     */
    private Object _lock = new Object();

    /**
     * For multicast receives on the MDNS port.
     */
    private MulticastSocket _receiveSocket;

    private InetAddress _intf;

    private NodelAutoDNS _host;
    
    public InterfaceAdvertiser(InetAddress intf, NodelAutoDNS host) {
        _logger = LoggerFactory.getLogger(this.getClass().getName() + "." + intf.getHostAddress().replace('.', '_'));
        
        _intf = intf;
        _host = host;
        
        // create the receiver thread and start it
        _multicastHandlerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                multicastReceiverThreadMain();
            }

        }, "probe_listener_thread");
        _multicastHandlerThread.setDaemon(true);
        _multicastHandlerThread.start();
    }

    /**
     * Creates a new socket, cleaning up if anything goes wrong in the process
     */
    private MulticastSocket createMulticastSocket(InetAddress intf, int port) throws Exception {
        MulticastSocket socket = null;

        try {
            _logger.info("Preparing socket to listen for probes. interface:{}, port:{}, group:{}", 
                    (intf == null ? "default" : intf), (port == 0 ? "any" : port), Discovery.MDNS_GROUP);
            
            socket = new MulticastSocket(new InetSocketAddress(intf, port)); // (port '0' means any port)

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
    private void multicastReceiverThreadMain() {
        while (_enabled) {
            MulticastSocket socket = null;

            try {
                socket = createMulticastSocket(_intf, Discovery.MDNS_PORT);
                
                synchronized(_lock) {
                    if (!_enabled)
                        Stream.safeClose(socket);
                    else
                        _receiveSocket = socket;
                }

                while (_enabled) {
                    DatagramPacket dp = UDPPacketRecycleQueue.instance().getReadyToUsePacket();

                    // ('returnPacket' will be called in 'catch' or later after use in thread-pool)

                    try {
                        socket.receive(dp);

                    } catch (Exception exc) {
                        UDPPacketRecycleQueue.instance().returnPacket(dp);

                        throw exc;
                    }
                    
                    InetAddress recvAddr = dp.getAddress();

                    if (recvAddr.isMulticastAddress()) {
                        Discovery.s_multicastInData.addAndGet(dp.getLength());
                        Discovery.s_multicastInOps.incrementAndGet();
                    } else {
                        Discovery.s_unicastInData.addAndGet(dp.getLength());
                        Discovery.s_unicastInOps.incrementAndGet();
                    }
                    
                    enqueueForProcessing(dp);
                    
                } // (inner while)

            } catch (Exception exc) {
                // (timeouts and general IO problems)
                
                // clean up regardless
                Stream.safeClose(socket);

                synchronized (_lock) {
                    if (!_enabled)
                        break;

                    _logger.warn("Receive failed; this may be a transitional condition. Will reinitialise... message was '" + exc.toString() + "'");
                    
                    // stagger retry
                    Threads.waitOnSync(_lock, 333);
                }
            }
        } // (outer while)

        _logger.info("This thread has run to completion.");
    } // (method)

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
                handleIncomingMessage((InetSocketAddress) dp.getSocketAddress(), message);
                
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
    
    /**
     * One responder per recipient at any stage.
     */
    private class Responder {
        
        public InetSocketAddress _recipient;
        
        private Iterator<ServiceItem> _serviceIterator;
        
        public Responder(InetSocketAddress recipient, Collection<ServiceItem> servicesSnapshot) {
            _recipient = recipient;
            
            // the service iterator
            _serviceIterator = servicesSnapshot.iterator();
        }
        
        /**
         * Starts the responder. Wait a random amount of time to before
         * actually sending anything. Staggered responses assists with receiver
         * buffer management by the remote receivers.
         */
        public void start(int randomResponseDelay) {
            // use a random minimum delay of 333ms
            int delay = 333;
            
            if (randomResponseDelay > 333)
                delay = randomResponseDelay;
            
            int timeToWait = Discovery.random().nextInt(delay);
            
            Discovery.timerThread().schedule(new TimerTask() {

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
            
            while(_serviceIterator.hasNext()) {
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
            message.addresses.add(Nodel.getTCPAddress());
            message.addresses.add(Nodel.getHTTPNodeAddress());
            
            // IO is involved so use thread-pool
            // (and only send if the 'present' list is not empty)
            
            if (message.present.size() > 0) {
                Discovery.threadPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        sendResponse(_receiveSocket, _recipient, message);
                    }

                });
            }
            
            // do we need this completed in the very near future?
            // if so, space out by at least 333ms.
            if (_serviceIterator.hasNext()) {
                
                Discovery.timerThread().schedule(new TimerTask() {

                    @Override
                    public void run() {
                        completeResponse();
                    }
                    
                }, 333);
            } else {
                synchronized(_responders) {
                    _responders.remove(_recipient);
                }
            }
        } // (method)
        
    } // (class)
    
    /**
     * Sends the message to a recipient 
     */
    private void sendResponse(DatagramSocket socket, InetSocketAddress to, NameServicesChannelMessage message) {
         _logger.info("Sending probe response. to={}, message={}", to, message);
        
        if (socket == null) {
            _logger.info("Not available yet; ignoring probe request.");
            return;
        }
        
        // convert into bytes
        String json = Serialisation.serialise(message);
        byte[] bytes = json.getBytes(UTF8Charset.instance());
        
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        packet.setSocketAddress(to);
        
        try {
            socket.send(packet);
            
            Discovery.s_unicastOutData.addAndGet(bytes.length);
            Discovery.s_unicastOutOps.incrementAndGet();

        } catch (IOException exc) {
            if (!_enabled)
                return;
            
            if (socket.isClosed())
                _logger.info("Send() ignored as socket is being recycled.");
            else
                _logger.warn("Send() failed. ", exc);
        }
    } // (method)
    
    /**
     * The map of responders by recipient address.
     */
    private ConcurrentMap<SocketAddress, Responder> _responders = new ConcurrentHashMap<SocketAddress, Responder>();

    /**
     * Handles a complete packet from the socket.
     */
    private void handleIncomingMessage(InetSocketAddress from, NameServicesChannelMessage message) {
        // discovery request?
        if (message.discovery != null) {
            _logger.info("Received probe packet. from={}, message={}", from, message);
            
            // create a responder if one isn't already active
            
            if (Nodel.getTCPAddress() == null) {
                _logger.info("(will not respond; nodel server port still not available)");
                
                return;
            }

            synchronized (_responders) {
                if (!_responders.containsKey(from)) {
                    Responder responder = new Responder(from, _host.getServicesSnapshot());
                    _responders.put(from, responder);

                    int delay = message.delay == null ? 0 : message.delay.intValue();
                    
                    responder.start(delay);
                }
            }
        } else {
            _logger.info("Received unexpected packet (expecting probe). from={}, message={}", from, message);            
        }
        
    } // (method)

    /**
     * Permanently shuts down all related resources.
     */
    public void shutdown() {
        // clear flag
        synchronized (_lock) {
            _enabled = false;
        }
        
        Stream.safeClose(_receiveSocket);
    }

}
