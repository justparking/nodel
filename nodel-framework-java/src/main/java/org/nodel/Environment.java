package org.nodel;

import java.io.IOException;

/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.Locale;

import org.nodel.io.Stream;

/**
 * Some utility methods related to the Java environment and OS its running on.
 */
public class Environment {
    
    /**
     * for:
     * ManagementFactory.getRuntimeMXBean()
     */
    private Object _runtimeMxBean;    
    
    /**
     * for:
     * List<String> arguments = runtimeMxBean.getInputArguments();
     */
    private Method _getInputArgumentsMethod;
    
    /**
     * for:
     * string name = ManagementFactory.getRuntimeMXBean().getName() 
     */
    private Method _getNameMethod;
    
    public enum OSType {
        Windows, MacOS, Linux, Other
    };
    
    /**
     * (see public getter)
     */
    private OSType _os;
    
    /**
     * An educated guess of the operating system.
     */
    public OSType getOperatingSystemType() {
        return this._os;
    }    
    
    /**
     * Ensures all the late binding only occurs once.
     */
    private Environment() {
        try {
            Class<?> factoryClass = Class.forName("java.lang.management.ManagementFactory");
            Method getRuntimeMXBeanMethod = factoryClass.getMethod("getRuntimeMXBean");
            
            _runtimeMxBean = getRuntimeMXBeanMethod.invoke(null, new Object[] {});
            
            Class<? extends Object> runtimeMxBeanClass = _runtimeMxBean.getClass();

            _getInputArgumentsMethod = runtimeMxBeanClass.getMethod("getInputArguments");
            _getInputArgumentsMethod.setAccessible(true);
            
            _getNameMethod = runtimeMxBeanClass.getMethod("getName");
            _getNameMethod.setAccessible(true);
            
        } catch (Exception exc) {
            // ignore
        }
        
        _os = guessOS();
    }

    /**
     * (singleton)
     */
    private static class LazyHolder {
        private static final Environment INSTANCE = new Environment();
    }

    /**
     * Shared, singleton instance.
     */
    public static Environment instance() {
        return LazyHolder.INSTANCE;
    }
    

    /**
     * Gets any special VM args that are used on every platform. We're looking to do this, but not all platforms 
     * support it so reflection is required:
     * 
     * java.lang.management.RuntimeMXBean runtimeMxBean = java.lang.management.ManagementFactory.getRuntimeMXBean();
     * List<String> arguments = runtimeMxBean.getInputArguments();
     */
    public List<String> getVMArgs() {
        try {
            if (_getInputArgumentsMethod == null)
                return null;
            
            Object argObj = _getInputArgumentsMethod.invoke(_runtimeMxBean, new Object[] {});

            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) argObj;

            return result;
            
        } catch (Exception exc) {
            return null;
        }
    }

    /**
     * In Java 7 and 8 there is no elegant way to get the current PID. This has to be done:
     * 
     * int result = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
     */
    public int getPID() {
        try {
            if (_getNameMethod == null)
                return 0;

            String name = (String) _getNameMethod.invoke(_runtimeMxBean, new Object[] {});

            int result = Integer.parseInt(name.split("@")[0]);

            return result;
            
        } catch (Exception exc) {
            return 0;
        }
    }
    
    /**
     * On OSX, Multicast creation using the constructor and interface parameter does not behave like on 
     * Windows or Linux. A "cannot assign address" exception is common.
     * 
     * The workaround is to use '.setInterface' after construction instead. But on Windows this does not 
     * explicitly bind to the given interface. It's the constructor, MulticastSocket(SocketAddress), that's what properly 
     * binds to a given interface (and not just 0.0.0.0).
     */
    public MulticastSocket createMulticastSocket(InetSocketAddress socketAddress) throws IOException {
        MulticastSocket ms = null;

        try {
            if (_os == OSType.MacOS) {
                // do a special workaround for MacOS
                ms = new MulticastSocket(socketAddress.getPort());
                ms.setInterface(socketAddress.getAddress());
                
            } else {
                // all other operating systems
                ms = new MulticastSocket(socketAddress);
                
            }
            
        } catch (Exception exc) {
            // ensure entire socket is closed if any exception is thrown
            Stream.safeClose(ms);
            
            throw exc;
        }
        
        return ms;
    }
    
    /**
     * (adapted from http://stackoverflow.com/questions/228477/how-do-i-programmatically-determine-operating-system-in-java)
     * (static convenience)
     */
    private static OSType guessOS() {
        String os = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH);
        if ((os.indexOf("mac") >= 0) || (os.indexOf("darwin") >= 0))
            return OSType.MacOS;

        else if (os.indexOf("win") >= 0)
            return OSType.Windows;

        else if (os.indexOf("nux") >= 0)
            return OSType.Linux;

        else
            return OSType.Other;
    }

} // (class)
