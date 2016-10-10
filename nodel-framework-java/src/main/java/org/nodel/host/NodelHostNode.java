package org.nodel.host;

import java.io.File;
import java.io.IOException;

import org.nodel.core.Nodel;
import org.nodel.diagnostics.Diagnostics;
import org.nodel.toolkit.Console;

/**
 * A special node to expose all system-level Nodel functions.
 */
public class NodelHostNode extends BaseDynamicNode {
    
    /**
     * (static lock)
     */
    private static Object s_lock = new Object();
    
    /**
     * The root this system node will work off
     */
    private static File s_root = new File(".nodel", "NodelHost-for-HOST");
    
    /**
     * Sets the root; must be called before 'instance()' to be effective.
     */
    public static void setInitialRoot(File root) {
        s_root = root;
    }

    public NodelHostNode(File root) throws IOException {
        super("NodelHost for " + Diagnostics.shared().hostname() + ":" + Nodel.getHTTPPort(), root);
    }
    
    public final Console.Interface console = new Console.Interface() {
        
        @Override
        public void warn(Object obj) {
            logWarning(String.valueOf(obj));
        }
        
        @Override
        public void log(Object obj) {
            NodelHostNode.this.log(String.valueOf(obj));
        }
        
        @Override
        public void info(Object obj) {
            logInfo(String.valueOf(obj));
        }
        
        @Override
        public void error(Object obj) {
            logError(String.valueOf(obj));
        }
        
    };
    
    /**
     * (singleton)
     */
    private static NodelHostNode s_instance;
    
    /**
     * The one and only instance.
     */
    public static NodelHostNode instance() {
        try {
            if (s_instance == null && s_lock != null) {
                synchronized (s_lock) {
                    if (s_instance == null)
                        s_instance = new NodelHostNode(s_root);
                }
            }

            return s_instance;
        } catch (Exception exc) {
            // make sure sub
            s_lock = null;
            throw new RuntimeException("System node could not be created.", exc);
        }
    }

}
