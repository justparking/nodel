package org.nodel.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.Collections;

import org.nodel.Handler;
import org.nodel.Threads;
import org.nodel.io.Stream;
import org.nodel.threading.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentBinder {
    
    /**
     * (init. in constructor)
     */
    private Logger _logger;
    
    /**
     * (convenience reference)
     */
    private ThreadPool _threadPool = Discovery.threadPool();
    
    /**
     * (synchronization)
     */
    private Object _lock = new Object();
    
    /**
     * (thread reference)
     */
    private Thread _mainThread;
    
    /**
     * The network interfaces managed by this class.
     */
    private NetworkInterface _intf;

    /**
     * If this should be shutdown
     * (synchronized)
     */
    private volatile boolean _shutdown = false;

    /**
     * (synchronized)
     */
    private MulticastSocket _socket;
    
    /**
     * (callbacks)
     */
    private Handler.H2<PersistentBinder, DatagramPacket> _packetHandler;

    /**
     * Adds a callback for topology changes (order is "new interfaces", "old interfaces")
     */
    public PersistentBinder setPacketHandler(Handler.H2<PersistentBinder, DatagramPacket> handler) {
        _packetHandler = handler;
        return this;
    }

    /**
     * After construction, attach callbacks and then start().
     */
    public PersistentBinder(NetworkInterface intf) {
        _logger = LoggerFactory.getLogger(PersistentBinder.class.getName() + "." + intf.getName());
        
        _intf = intf;
    }

    /**
     * Called after callbacks attached.
     */
    public PersistentBinder start() {
        synchronized (_lock) {
            if (_shutdown)
                throw new IllegalStateException("Already started.");

            // init and receive thread
            _mainThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    threadMain();
                }

            }, "InterfaceBinding_" + _intf.getName());

            _mainThread.start();

            return this;
        }
    }

    /**
     * (thread entry-point)
     */
    private void threadMain() {
        while (!_shutdown) {
            MulticastSocket socket = null;
            
            try {
                socket = init();

                synchronized (_lock) {
                    if (!_shutdown)
                        _socket = socket;
                }

                listen(socket);

            } catch (Exception exc) {
                // cleanup always
                Stream.safeClose(socket);

                if (_shutdown)
                    break;

                _logger.warn("\"" + exc.getMessage() + "\"; will back off for 30s.", exc);

                Threads.safeWait(_lock, 30000);
            }
        } // while

        _logger.info("Binder is shutdown (main thread run to completion)");
    }

    private MulticastSocket init() {
        MulticastSocket socket = null;

        try {
            socket = new MulticastSocket(Discovery.MDNS_PORT);

            // "binds" to a specific interface
            // (this is done deliberately instead of with constructor because of issues with OSX. See code history.)
            socket.setNetworkInterface(_intf);

            // will definitely have IPv4
            socket.joinGroup(Discovery._group);

            _logger.info("Multicast socket created. address:{} port:{} group:{}", getIPv4Addresses(_intf), Discovery.MDNS_PORT, Discovery.MDNS_GROUP);

            return socket;

        } catch (Exception exc) {
            // cleanup always
            Stream.safeClose(socket);

            throw new RuntimeException("Problem setting up multicast socket", exc);
        }
    }

    private void listen(MulticastSocket socket) throws IOException {
        DatagramPacket packet = UDPPacketRecycleQueue.instance().getReadyToUsePacket();

        try {
            while (!_shutdown) {
                packet.setLength(packet.getData().length);

                socket.receive(packet);

                // offload immediately to maximize port availability
                Handler.handle(_packetHandler, this, packet);

                if (_logger.isDebugEnabled())
                    _logger.debug("Got packet. from:{} data:[{}]", packet.getSocketAddress(), new String(packet.getData(), 0, packet.getLength()));

            } // (while)
            
        } finally {
            UDPPacketRecycleQueue.instance().returnPacket(packet);
        }
    }

    /**
     * Sends a packet (can block and/or throw exceptions)
     */
    public void send(DatagramPacket packet) throws IOException {
        try {
            MulticastSocket socket;
            synchronized (_lock) {
                socket = _socket;
            }

            if (socket != null)
                socket.send(packet);
        } catch (Exception exc) {
            synchronized (_lock) {
                if (_shutdown)
                    return;

                throw exc;
            }
        }
    }

    /**
     * Permanently shuts down this binder.
     */
    public void shutdown() {
        synchronized(_lock) {
            _shutdown = true;

            _threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    Stream.safeClose(_socket);
                }
            });
        }
    }
    
    /**
     * (convenience)
     */
    @Override
    public String toString() {
        return _intf.getName();
    }
    
    /**
     * (convenience function)
     */
    private static StringBuilder getIPv4Addresses(NetworkInterface intf) {
        // for information purposes
        StringBuilder addressesText = new StringBuilder();
        for (InetAddress address : Collections.list(intf.getInetAddresses())) {
            if (!(address instanceof Inet4Address))
                continue;

            if (addressesText.length() > 0)
                addressesText.append(",");

            addressesText.append(address.getHostAddress());
        }
        return addressesText;
    }
    
}
