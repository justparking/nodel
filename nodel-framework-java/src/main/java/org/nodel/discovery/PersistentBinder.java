package org.nodel.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nodel.Handler;
import org.nodel.Threads;
import org.nodel.io.Stream;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.Timers;
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
     * (convenience reference)
     */
    private Timers _timerThread = Discovery.timerThread();
    
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
    private boolean _shutdown = false;

    /**
     * (synchronized)
     */
    private MulticastSocket _socket;

    /**
     * (completely non-blocking)
     */
    public PersistentBinder(NetworkInterface intf) {
        _logger = LoggerFactory.getLogger(PersistentBinder.class.getName() + "." + intf.getName());
        
        _intf = intf;
        
        // kick off init and receive thread
        _mainThread = new Thread(new Runnable() {

            @Override
            public void run() {
                threadMain();
            }
            
        }, "InterfaceBinding_" + intf.getName());
        
        _mainThread.start();
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

                _logger.warn("{}; will back off for 30s.", exc.getMessage(), exc);

                Threads.safeWait(_lock, 30000);
            }
        } // while
        
        _logger.info("(thread run to completion)");
    }
    
    private MulticastSocket init() {
        MulticastSocket socket = null;
        
        try {
            int port = Discovery.MDNS_PORT;
            socket = new MulticastSocket(port);
            
            socket.setNetworkInterface(_intf);
            
            boolean hasIPv6 = false;
            
            StringBuilder addressesText = new StringBuilder();
            
            for (InetAddress address : Collections.list(_intf.getInetAddresses())) {
                if (address instanceof Inet6Address)
                    hasIPv6 = true;
                
                if (addressesText.length() > 0)
                    addressesText.append(",");
                
                addressesText.append(address.getHostAddress());
            }

            // will definitely have IPv4
            socket.joinGroup(Discovery._group);
            
            // do IPv6 too if addr present
            if (hasIPv6)
                socket.joinGroup(Discovery._group_v6);

            if (hasIPv6)
                _logger.info("Multicast socket created. port:{} addresses:[{}] groups:[{},{}]", port, addressesText, Discovery.MDNS_GROUP, Discovery.MDNS_GROUP_IPV6);
            else
                _logger.info("Multicast socket created. port:{} addresses:[{}] group:{}", port, addressesText, Discovery.MDNS_GROUP);
            
            return socket;
            
        } catch (Exception exc) {
            // cleanup always
            Stream.safeClose(socket);
            
            throw new RuntimeException("Problem setting up multicast socket", exc);
        }        
    }
    
    private void listen(MulticastSocket socket) throws IOException {
        DatagramPacket packet = new DatagramPacket(new byte[10240], 10240);
        
        while (!_shutdown) {
            packet.setLength(10240);
            socket.receive(packet);
            
            System.out.println("Got packet. from:" + packet.getSocketAddress() + ", data:[" + new String(packet.getData(), 0, packet.getLength()) + "]");
        }
    }

    public void shutdown() {
    }

    public static void main(String[] args) throws IOException  {
        final Map<NetworkInterface, PersistentBinder> intfs = new HashMap<NetworkInterface, PersistentBinder>();
        
        TopologyMonitor.instance().addOnChangeHandler(new Handler.H2<List<NetworkInterface>, List<NetworkInterface>>() {
            
            @Override
            public void handle(List<NetworkInterface> newly, List<NetworkInterface> gone) {
                System.out.println("NEW:" + newly);
                System.out.println("GONE:" + gone);

                for(NetworkInterface intf: newly)
                    intfs.put(intf, new PersistentBinder(intf));
                
                for(NetworkInterface intf: gone)
                    intfs.remove(intf).shutdown();
            }
            
        });
        
        System.in.read();
    }
    
}
