package org.nodel.jyhost;

/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownServiceException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.nodel.Handler;
import org.nodel.SimpleName;
import org.nodel.Strings;
import org.nodel.core.Nodel;
import org.nodel.core.NodelClients.NodeURL;
import org.nodel.diagnostics.Diagnostics;
import org.nodel.discovery.AdvertisementInfo;
import org.nodel.discovery.AutoDNS;
import org.nodel.discovery.TopologyWatcher;
import org.nodel.host.BaseDynamicNode;
import org.nodel.host.BaseNode;
import org.nodel.host.NanoHTTPD;
import org.nodel.io.Stream;
import org.nodel.io.UTF8Charset;
import org.nodel.json.XML;
import org.nodel.logging.LogEntry;
import org.nodel.logging.Logging;
import org.nodel.reflection.Param;
import org.nodel.reflection.Serialisation;
import org.nodel.reflection.SerialisationException;
import org.nodel.reflection.Service;
import org.nodel.reflection.Value;
import org.nodel.rest.EndpointNotFoundException;
import org.nodel.rest.REST;
import org.nodel.threading.ThreadPond;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyException;
import org.python.core.PyStringMap;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodelHostHTTPD extends NanoHTTPD {

    /**
     * (logging related)
     */
    private static AtomicLong s_instance = new AtomicLong();

    /**
     * (logging related)
     */
    protected long _instance = s_instance.getAndIncrement();

    /**
     * (logging related)
     */
    protected Logger _logger = LoggerFactory.getLogger(this.getClass().getName() + "_" + _instance);
    
    /**
     * The last user agent being used.
     */
    private String _userAgent;
    
    /**
     * Represents the model exposed to the REST services.
     */
    public class RESTModel {
        
        @Service(name = "nodes", order = 1, title = "Nodes", desc = "Node lookup by Node name.", genericClassA = SimpleName.class, genericClassB = BaseNode.class)
        public AbstractMap<SimpleName, BaseNode> nodes = new AbstractMap<SimpleName, BaseNode>() {

            @Override
            public Set<Map.Entry<SimpleName, BaseNode>> entrySet() {
                return BaseNode.getNodes().entrySet();
            }
            
            @Override
            public BaseNode get(Object key) {
                return BaseNode.getNode((SimpleName) key);
            }
            
        };
        
        @Value(name = "nodes", order = 1, title = "Nodes", desc = "All the managed nodes.", genericClassA = SimpleName.class, genericClassB = BaseNode.class)
        public Map<SimpleName, BaseNode> getNodes() {
            return BaseNode.getNodes();
        }
        
        @Service(name = "recipes", order = 1, title = "Recipes", desc = "Recipes that new nodes can be based on", genericClassA = String.class)
        public RecipesEndPoint recipes() {
            return _nodelHost.recipes();
        }
        
        @Value(name = "started", title = "Started", desc = "When the host started.")
        public DateTime __started = DateTime.now();
        
        @Service(name = "allNodes", order = 5, title = "All nodes", desc = "Returns all the advertised nodes.")
        public Collection<AdvertisementInfo> getAllNodes() {
            return _nodelHost.getAdvertisedNodes();
        }
        
        @Service(name = "discovery", order = 6, title = "Discovery service", desc = "Multicast discovery services.")
        public AutoDNS discovery() {
            return AutoDNS.instance();
        }        
        
        @Service(name = "nodeURLs", order = 6, title = "Node URLs", desc = "Returns the addresses of all advertised nodes.")
        public List<NodeURL> nodeURLs(@Param(name = "filter", title = "Filter", desc = "Optional string filter.") String filter) throws IOException {
            return _nodelHost.getNodeURLs(filter);
        }
        
        @Service(name = "nodeURLsForNode", order = 6, title = "Node URLs", desc = "Returns the addresses of all advertised nodes.")
        public List<NodeURL> nodeURLsForNode(@Param(name = "name") SimpleName name) throws IOException {
            return _nodelHost.getNodeURLsForNode(name);
        }

        @Service(name = "logs", title = "Logs", desc = "Detailed program logs.")
        public LogEntry[] getLogs(
                @Param(name = "from", title = "From", desc = "Start inclusion point.") long from, 
                @Param(name = "max", title = "Max", desc = "Results count limit.") int max) {
            List<LogEntry> result = Logging.instance().getLogs(from, max);
            
            return result.toArray(new LogEntry[result.size()]);
        } // (method)

        @Service(name = "warningLogs", title = "Warning logs", desc = "Same as 'logs' except filtered by warning-level.")
        public LogEntry[] getWarningLogs(
                @Param(name = "from", title = "From", desc = "Start inclusion point.") long from, 
                @Param(name = "max", title = "Max", desc = "Results count limit.") int max) {
            List<LogEntry> result = Logging.instance().getWarningLogs(from, max);

            return result.toArray(new LogEntry[result.size()]);
        } // (method)
        
        @Service(name = "diagnostics", order = 6, title = "Diagnostics", desc = "Diagnostics related to the entire framework.")
        public Diagnostics framework() {
            return Diagnostics.shared();
        }

        @Service(name = "newNode", order = 7, title = "New node", desc = "Creates a new node.")
        public void newNode(@Param(name = "base") String base, SimpleName name) {
            _nodelHost.newNode(base, name);
        }
        
        @Service(name = "toolkit", title = "Toolkit", desc = "The toolkit reference.")
        public Info getToolkitReference() throws IOException {
            try (InputStream nodetoolkitStream = PyNode.class.getResourceAsStream("nodetoolkit.py")) {
                Info info = new Info();
                info.script = Stream.readFully(nodetoolkitStream);
                return info;
            }
        }

    } // (inner class)
    
    public static class Info {

        @Value(name = "script")
        public String script;

    }

    private NodelHost _nodelHost;

    /**
     * Holds the object bound to the REST layer
     */
    private RESTModel _restModel = new RESTModel();
    
    public NodelHostHTTPD(int port, File directory) throws IOException {
        super(port, directory, false);

        // update with actual listening port
        Nodel.setHTTPPort(getListeningPort());
        
        // and watch for future interface changes
        TopologyWatcher.shared().addOnChangeHandler(new TopologyWatcher.ChangeHandler() {

            @Override
            public void handle(List<InetAddress> appeared, List<InetAddress> disappeared) {
                handleTopologyChange(appeared, disappeared);
            }

        });
    }

    /**
     * When the interfaces topology changes, the public IP address might change too.
     */
    private void handleTopologyChange(List<InetAddress> appeared, List<InetAddress> disappeared) {
        InetAddress[] addresses = TopologyWatcher.shared().getInterfaces();
        
        String[] httpAddresses = new String[addresses.length];
        String[]  httpNodeAddresses = new String[addresses.length];

        for (int a = 0; a < addresses.length; a++) {
            httpAddresses[a] = String.format("http://%s:%s%s", addresses[a].getHostAddress(), Nodel.getHTTPPort(), Nodel.getHTTPSuffix());
            httpNodeAddresses[a] = String.format("http://%s:%s", addresses[a].getHostAddress(), Nodel.getHTTPPort());
        }

        Nodel.updateHTTPAddresses(httpAddresses, httpNodeAddresses);

        for (InetAddress newly : appeared) {
            System.out.println("    (web interface available at " + String.format("http://%s:%s", newly.getHostAddress(), Nodel.getHTTPPort()) + ")\n");
        }

        for (InetAddress gone : disappeared)
            System.out.println("    (" + gone.getHostAddress() + " interface disappeared)");
    }
    
    /**
     * Sets the host.
     */
    public void setNodeHost(NodelHost value) {
        _nodelHost = value;
    }

    /**
     * (all headers are stored by lower-case)
     */
    @Override
    public Response serve(String uri, File root, String method, Properties params, Request request, Handler.H1<Response> onComplete) {
        _logger.debug("Serving '" + uri + "'...");

        // if REST being used, the target object
        Object restTarget = _restModel;
        ThreadPond threadPool = null;

        // get the user-agent
        String userAgent = request.headers.getProperty("user-agent");
        if (userAgent != null)
            _userAgent = userAgent;

        // get the parts (avoiding blank first part if necessary)
        String[] parts = (uri.startsWith("/") ? uri.substring(1) : uri).split("/");

        // check if we're serving up from a node root
        // 'http://example/nodes/index.htm'
        if (parts.length >= 2 && parts[0].equalsIgnoreCase("nodes")) {
            // the second part will be the node name
            SimpleName nodeName = new SimpleName(parts[1]);

            BaseNode node = BaseNode.getNode(nodeName);
            if (node instanceof BaseDynamicNode)
                threadPool = ((BaseDynamicNode)node).getThreadPool();

            if (node == null)
                return prepareNotFoundResponse(uri, "Node");

            // check if properly formed URI is being used i.e. ends with slash
            if (parts.length == 2 && !uri.endsWith("/"))
                return prepareRedirectResponse(encodeUri(uri + "/"));

            File nodeRoot = node.getRoot();
            root = new File(nodeRoot, "content");

            restTarget = node;

            // rebuild the 'uri' and 'parts'
            int OFFSET = 2;

            StringBuilder sb = new StringBuilder();
            String[] newParts = new String[parts.length - OFFSET];

            for (int a = OFFSET; a < parts.length; a++) {
                String path = parts[a];

                sb.append('/');
                sb.append(path);

                newParts[a - OFFSET] = path;
            }

            if (sb.length() == 0)
                sb.append('/');

            uri = sb.toString();
            parts = newParts;
        }

        // check if REST is being used
        if (parts.length > 0 && parts[0].equals("REST")) {
            // drop 'REST' part
            int OFFSET = 1;
            String[] newParts = new String[parts.length - OFFSET];
            for (int a = OFFSET; a < parts.length; a++)
                newParts[a - OFFSET] = parts[a];

            parts = newParts;

            String[] finalParts = parts;
            Object finalRestTarget = restTarget;

            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    try {
                        Object target;

                        if (method.equalsIgnoreCase("GET"))
                            target = REST.resolveRESTcall(finalRestTarget, finalParts, params, null);

                        else if (method.equalsIgnoreCase("POST"))
                            target = REST.resolveRESTcall(finalRestTarget, finalParts, params, request.raw);

                        else
                            throw new UnknownServiceException("Unexpected method - '" + method + "'");

                        // check if the target is an HTTP directive
                        Response resp;
                        if (target instanceof Response) {
                            resp = (Response) target;

                        } else {
                            // otherwise serialise the target into JSON
                            String targetAsJSON = Serialisation.serialise(target);
                            resp = new Response(HTTP_OK, "application/json; charset=utf-8", targetAsJSON);
                        }

                        // adjust the response headers for script compatibility
                        resp.addHeader("Access-Control-Allow-Origin", "*");

                        onComplete.handle(resp);
                        return;

                    } catch (EndpointNotFoundException exc) {
                        onComplete.handle(prepareExceptionMessageResponse(HTTP_NOTFOUND, exc, false));
                        return;

                    } catch (FileNotFoundException exc) {
                        onComplete.handle(prepareExceptionMessageResponse(HTTP_NOTFOUND, exc, false));
                        return;

                    } catch (SerialisationException exc) {
                        onComplete.handle(prepareExceptionMessageResponse(HTTP_INTERNALERROR, exc, params.containsKey("trace")));
                        return;

                    } catch (UnknownServiceException exc) {
                        onComplete.handle(prepareExceptionMessageResponse(HTTP_INTERNALERROR, exc, false));
                        return;

                    } catch (PyException exc) {
                        // use cleaner PyException stack trace
                        _logger.warn("Python script exception during REST operation. {}", exc.toString());

                        onComplete.handle(prepareExceptionMessageResponse(HTTP_INTERNALERROR, exc, params.contains("trace")));
                        return;

                    } catch (Exception exc) {
                        _logger.warn("Unexpected exception during REST operation.", exc);

                        onComplete.handle(prepareExceptionMessageResponse(HTTP_INTERNALERROR, exc, params.contains("trace")));
                        return;
                    }
                }

            };

            if (threadPool != null)
                threadPool.execute(runnable);
            else
                runnable.run();

            // will use onComplete callback instead
            return null;

        } else {
            // TODO: this could be done a lot better:
            
            Response response = null;
            
            if (params.containsKey("_edit")) {
                return super.serve("/editor.htm", root, method, params, request, onComplete);
                
            } else if (params.containsKey("_source")) {
                
                File target = resolveFile(uri, root);
                if (target == null)
                    return new Response(HTTP_NOTFOUND, "text/plain", "Not found - " + uri);
                else
                    return new Response(HTTP_OK, "text/plain; charset=utf-8", Stream.tryReadFully(target));
                
            } else if (params.containsKey("_write")) {
                File target = resolveFile(uri, root);
                if (target == null)
                    return new Response(HTTP_NOTFOUND, "text/plain", "Not found - " + uri);

                if (request.raw == null || request.raw.length == 0)
                    return new Response(HTTP_FORBIDDEN, "text/plain", "No POST data provided.");

                FileOutputStream fos = null;
                
                try {
                    fos = new FileOutputStream(target);
                    
                    // TODO: should backup files here
                    
                    fos.write(request.raw);
                    
                    return new Response(HTTP_OK, "text/plain", request.raw.length + " bytes written.");
                    
                } catch (Exception exc) {
                    return new Response(HTTP_INTERNALERROR, "text/plain", "Problem writing file.");

                } finally {
                    Stream.safeClose(fos);
                }
            } 

            // not a REST call, py-server page page?
            if (restTarget instanceof PyNode) {
                if (uri.endsWith(".pysp"))
                    // try actual page 
                    response = handlePySp((PyNode) restTarget, uri, root, method, params, request, onComplete);
                else
                    // try as '.pysp'
                    response = handlePySp((PyNode) restTarget, uri + ".pysp", root, method, params, request, onComplete);
            }

            if (response == null || HTTP_NOTFOUND.equals(response.status))
                return super.serve(uri, root, method, params, request, onComplete);

            return response;
        }
    } // (method)

    /**
     * An exception message
     */
    public class ExceptionMessage {

        @Value(name = "code")
        public String code;

        @Value(name = "error")
        public String error;

        @Value(name = "message")
        public String message;

        @Value(name = "cause")
        public ExceptionMessage cause;

        @Value(name = "stackTrace")
        public String stackTrace;

    } // (class)

    /**
     * Prepares a neat exception tree for returning back to the HTTP client.
     */
    private Response prepareExceptionMessageResponse(String httpCode, Exception exc, boolean includeStackTrace) {
        assert exc != null : "Argument should not be null."; 
        
        ExceptionMessage message = new ExceptionMessage();
        
        Throwable currentExc = exc;
        ExceptionMessage currentMessage = message;
        
        while(currentExc != null) {
            currentMessage.error = currentExc.getClass().getSimpleName();
            currentMessage.message = currentExc.getMessage();
            if (Strings.isNullOrEmpty(currentMessage.message))
                currentMessage.message = currentExc.toString();
            
            if (includeStackTrace) {
                currentMessage.stackTrace = captureStackTrace(currentExc);
                
                // only capture it once
                includeStackTrace = false;
            }
            
            if (currentExc.getCause() == null)
                break;
            
            currentExc = currentExc.getCause();
            currentMessage.cause = new ExceptionMessage();
            
            currentMessage = currentMessage.cause;
        } // (while)
        
        Response resp = new Response(httpCode, "application/json; charset=utf-8", Serialisation.serialise(message));
        resp.addHeader("Access-Control-Allow-Origin", "*");
        
        return resp;
    } // (method)
    
    
    /**
     * Prepares a standard 404 Not Found HTTP response.
     * @type e.g. 'Node' or 'Type' (capitalise first letter)
     */
    private Response prepareNotFoundResponse(String path, String type) {
		ExceptionMessage errorResponse = new ExceptionMessage();
		
		errorResponse.error = "NotFound";
		// e.g. "Path '___' was not found." or
		// "Node '__' was not found."
		errorResponse.message = type + " '" + path + "' was not found.";
		errorResponse.code = "404";
		
		return new Response(HTTP_NOTFOUND, "application/json; charset=utf-8", Serialisation.serialise(errorResponse));
    }
    
    /**
     * Captures an exception's stack-trace.
     */
    private static String captureStackTrace(Throwable currentExc) {
    	StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        
        currentExc.printStackTrace(pw);
        
        pw.flush();
        return sw.toString();
    }
    
    /**
     * Returns the last user-agent in use. Could be null if not set yet.
     */
    public String getUserAgent() {
        return _userAgent;
    }
    
    /**
     * ThreadLocal is used here so can be static.
     */
    private static ThreadLocal<PyStringMap> s_locals = new ThreadLocal<PyStringMap>() {
        
        /**
         * Returns a string map when first used.
         */
        protected PyStringMap initialValue() {
            return new PyStringMap();
        }
        
    };
    
    public static class ServerPageResponse {
        
        public String status;
        
        public String mimeType;
        
        /**
         * Headers for the HTTP response. Use addHeader() to add lines.
         */
        public Properties headers = new Properties();
        
        private StringBuilder _sb = new StringBuilder();
        
        /**
         * Adds given line to the header.
         */
        public void addHeader(String name, String value) {
            headers.put(name, value);
        }

        /**
         * Convenience method that makes an InputStream out of given text.
         */
        public ServerPageResponse() {
        }
        
        public void print(Object value) {
            _sb.append(value);
        }
        
        public void println() {
            _sb.append(System.lineSeparator());
        }
        
        public void println(Object value) {
            _sb.append(value).append(System.lineSeparator());
        }
        
        public void escape(Object value) {
            String escaped = value != null ? XML.escape(value.toString()) : ""; 
            _sb.append(escaped);
        }
        
        public String getData() {
            return _sb.toString();
        }
        
    }
    
    /**
     * @param node (pre-checked)
     * @return 
     */
    private Response handlePySp(final PyNode node, String uri, File root, String method, Properties params, final Request request, Handler.H1<Response> onComplete) {
        // resolve the file
        // Note: the response will be HTTP_OK or some other error (content unchanged, partial requests etc. will never occur).
        Response originalResponse = super.serve(uri, root, method, params, request, true, onComplete);
        
        // only deal with things if an HTTP_OK is received
        if (!HTTP_OK.equalsIgnoreCase(originalResponse.status))
                return originalResponse;
        
        final ServerPageResponse response = new ServerPageResponse();
        response.status = HTTP_OK;
        response.mimeType = "text/html";
        
        PythonInterpreter python = node.getPython();
        
        // this is safe because using thread-local storage
        PyStringMap locals = s_locals.get();
        
        final String responseVariable = "resp";
        
        final StringBuilder scriptBuilder = new StringBuilder();
        
        try {
            String template = Stream.readFully(new InputStreamReader(originalResponse.data, UTF8Charset.instance()));

            final Throwable[] exceptionHolder = new Exception[1];
            
            ServerSideFilter filter = new ServerSideFilter(template) {
                
                char lastLine = ' ';
                
                @Override
                public void resolveExpression(String expr) throws Throwable {
                    if (lastLine == 'e' || lastLine == 'p')
                        scriptBuilder.append("; \\\r\n");
                    
                    scriptBuilder.append(responseVariable).append(".print(").append(expr).append(")");
                    lastLine = 'e';
                }

                @Override
                public void evaluateBlock(String block) throws Throwable {
                    scriptBuilder.append(block);
                    lastLine = 'b';
                }

                @Override
                public void passThrough(String data) throws Throwable {
                    if (lastLine == 'e' || lastLine == 'p')
                        scriptBuilder.append("; \\\r\n");
                                
                    scriptBuilder.append(responseVariable).append(".print('" + data + "')");
                    lastLine = 'p';
                }

                @Override
                public void handleError(Throwable th) {
                    exceptionHolder[0] = th;
                }

                @Override
                public void comment(String comment) throws Throwable {
                }

                @Override
                public void resolveEscapedExpression(String expr) throws Throwable {
                    if (lastLine == 'e' || lastLine == 'p')
                        scriptBuilder.append("; \\\r\n");

                    scriptBuilder.append(responseVariable).append(".escape(").append(expr).append(")");
                    lastLine = 'e';
                }

            };
            filter.process();
            
            String script = scriptBuilder.toString();
            
            // TODO: convert this to a class resource
            
            if (params.containsKey("_compiled"))
                return new Response(HTTP_OK, "text/plain; charset=utf-8", script);
            
            locals.clear();
            locals.__setitem__("req".intern(), Py.java2py(request));
            locals.__setitem__(responseVariable.intern(), Py.java2py(response));
            
            if (exceptionHolder[0] != null) {
                node.injectError("Ignoring PySp parse error", exceptionHolder[0]);
            }
            
            python.setLocals(locals);
            
            // ('systemState' is set within 'compile'...)
            PyCode pyCode = python.compile(script);
            
            Py.exec(pyCode, node.getPyGlobals(), locals);
            
            Response nanoResponse = new Response(response.status, response.mimeType, response.getData());
            nanoResponse.header = response.headers;

            return nanoResponse;
            
        } catch (Exception exc) {
            _logger.warn("Unexpected exception during PySP filter handling URI:" + uri, exc);

            return prepareExceptionMessageResponse(HTTP_INTERNALERROR, exc, params.contains("trace"));
            
        } finally {
            try {
                python.getLocals().__delitem__(responseVariable.intern());
            } catch (Exception ignore) {
            }         
        }
    }

} // (class)
