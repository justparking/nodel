package org.nodel.toolkit;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;
import org.nodel.Handler;
import org.nodel.Handler.H0;
import org.nodel.Handler.H1;
import org.nodel.Strings;
import org.nodel.Threads;
import org.nodel.diagnostics.Diagnostics;
import org.nodel.diagnostics.SharableMeasurementProvider;
import org.nodel.host.BaseNode;
import org.nodel.threading.CallbackQueue;
import org.nodel.threading.ThreadPool;
import org.nodel.threading.TimerTask;
import org.nodel.threading.Timers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Written like Managed TCP where possible but for WebSockets.
 *
 *  Features include:
 *  - staggered start up (prevent startup storms)
 *  - event-based
 *  - efficient stream filtering
 *    - minimisation of String object fragmentation
 *    - automatic delimiting
 *    - UTF8 decoded
 *    - trimmed
 *  - exponential back-off
 */
public class ManagedWebSocket implements Closeable {

    private static final AtomicLong s_instanceCounter = new AtomicLong();

    /**
     * (used by 'logger' and thread name)
     */
    private final long _instance = s_instanceCounter.getAndIncrement();

    /**
     * (logging related)
     */
    private final Logger _logger = LoggerFactory.getLogger(String.format("%s.instance%d", this.getClass().getName(), _instance));

    /**
     * The kick-off delay (randomized)
     */
    private final static int KICKOFF_DELAY = 5000;

    /**
     * To randomise the kick-off delay
     */
    private final static Random s_random = new Random();

    /**
     * The minimum gap between connections (default 500ms)
     * A minimum gap is used to achieve greater reliability when connecting to  hosts
     * that may be TCPIP stack challenged (think older devices, projectors, etc.)
     */
    private final static int MIN_CONNETION_GAP = 500;

    /**
     * The maximum back-off time allowed (default 32 secs or 2^5 millis)
     */
    private final static int MAX_BACKOFF = 32000;

    /**
     * The connection or TCP read timeout (default 5 mins)
     */
    private static final int RECV_TIMEOUT =  5 * 60000;

    /**
     * The amount of time given to connect to a socket.
     */
    private static final int CONNECT_TIMEOUT = 30000;

    /**
     * (synchronisation / locking)
     */
    private final Object _lock = new Object();

    /**
     * The shared thread-pool
     */
    private final ThreadPool _threadPool;

    /**
     * The safe queue as provided by a host
     */
    private final CallbackQueue _callbackHandler;

    /**
     * The current back-off period (exponential back-off)
     */
    private int _backoffTime = 0;

    /**
     * If there was a recent successful connection.
     */
    private boolean _recentlyConnected = false;

    /**
     * (see setter)
     */
    private H0 _connectedCallback;

    /**
     * (see setter)
     */
    private H0 _disconnectedCallback;

    /**
     * (see setter)
     */
    private H1<String> _receivedCallback;

    /**
     * (see setter)
     */
    private H1<String> _sentCallback;

    /**
     * (see setter)
     */
    private H0 _timeoutCallback;

    /**
     * When errors occur during callbacks.
     */
    private final H1<Exception> _callbackErrorHandler;

    /**
     * Permanently shut down?
     */
    private boolean _shutdown;

    /**
     * Has started?
     */
    private boolean _started;

    /**
     * Shared timer framework to use.
     */
    private final Timers _timerThread;

    /**
     * The current web socket.
     * (locked with '_lock')
     */
    private WebSocketClient _webSocket;

    /**
     * The start timer.
     */
    private TimerTask _startTimer;

    /**
     * Holds the full socket address (addr:port)
     * (may be null)
     */
    private String _dest;

    /**
     * The default request timeout value (timed from respective 'send')
     */
    private int _requestTimeout = 10000;

    /**
     * The request timeout value (timed from respective 'queue')
     */
    private int _longTermRequestTimeout = 60000;

    /**
     * The last time a successful connection occurred (connection or data receive)
     * (nano time)
     */
    private long _lastSuccessfulConnection = System.nanoTime();

    /**
     * (Response for handling thread-state)
     */
    private final H0 _threadStateHandler;

    /**
     * The connection and receive timeout.
     */
    private int _timeout = RECV_TIMEOUT;

    /**
     * (diagnostics)
     */
    private final SharableMeasurementProvider _counterConnections;

    /**
     * (diagnostics)
     */
    private final SharableMeasurementProvider _counterRecvOps;

    /**
     * (diagnostics)
     */
    private SharableMeasurementProvider _counterRecvRate;

    /**
     * (diagnostics)
     */
    private SharableMeasurementProvider _counterSendOps;

    /**
     * (diagnostics)
     */
    private SharableMeasurementProvider _counterSendRate;

    /**
     * The request queue
     */
    private final ConcurrentLinkedQueue<QueuedRequest> _requestQueue = new ConcurrentLinkedQueue<QueuedRequest>();

    /**
     * Holds the queue length ('ConcurrentLinkedQueue' not designed to handle 'size()' efficiently)
     */
    private int _queueLength = 0;

    /**
     * The active request
     */
    private QueuedRequest _activeRequest;

    /**
     * (see setter)
     */
    private Map<String, String> _headers;

    /**
     * (see setter)
     */
    public Map<String, String> getHeaders() {
        return _headers;
    }

    /**
     * Extra/override HTTP headers on connection
     */
    public void setHeaders(Map<String, String> value) {
        _headers = value;
    }

    /**
     * (constructor)
     */
    public ManagedWebSocket(BaseNode node, String dest, H0 threadStateHandler, H1<Exception> callbackExceptionHandler, CallbackQueue callbackQueue, ThreadPool threadPool, Timers timers) {
        _dest = dest;

        _threadStateHandler = threadStateHandler;
        _callbackErrorHandler = callbackExceptionHandler;
        _callbackHandler = callbackQueue;
        _threadPool = threadPool;
        _timerThread = timers;

        // register the counters
        String counterName = "'" + node.getName().getReducedName() + "'";
        _counterConnections = Diagnostics.shared().registerSharableCounter(counterName + ".TCP connects", true);
        _counterRecvOps = Diagnostics.shared().registerSharableCounter(counterName + ".TCP receives", true);
        _counterRecvRate = Diagnostics.shared().registerSharableCounter(counterName + ".TCP receive rate", true);
        _counterSendOps = Diagnostics.shared().registerSharableCounter(counterName + ".TCP sends", true);
        _counterSendRate = Diagnostics.shared().registerSharableCounter(counterName + ".TCP send rate", true);
    }

    /**
     * When a connection moves into a connected state.
     */
    public void setConnectedHandler(H0 handler) {
        _connectedCallback = handler;
    }

    /**
     * When the connected moves into a disconnected state.
     */
    public void setDisconnectedHandler(H0 handler) {
        _disconnectedCallback = handler;
    }

    /**
     * When a data segment arrives.
     */
    public void setReceivedHandler(H1<String> handler) {
        _receivedCallback = handler;
    }

    /**
     * When a data segment is sent
     */
    public void setSentHandler(H1<String> handler) {
        _sentCallback = handler;
    }

    /**
     * When a a connector a request timeout occurs.
     */
    public void setTimeoutHandler(H0 handler) {
        _timeoutCallback = handler;
    }

    /**
     * Sets the destination.
     */
    public void setDest(String dest) {
        synchronized(_lock) {
            _dest = dest;
        }
    }

    /**
     * Sets the connection and receive timeout.
     */
    public void setTimeout(int value) {
        synchronized(_lock) {
            _timeout = value;
        }
    }

    /**
     * Gets the connection and receive timeout.
     */
    public int getTimeout() {
        return _timeout;
    }

    /**
     * The request timeout (millis)
     */
    public int getRequestTimeout() {
        return _requestTimeout;
    }

    /**
     * The request timeout (millis)
     */
    public void setRequestTimeout(int value) {
        _requestTimeout = value;
    }

    /**
     * Returns the current queue length size.
     */
    public int getQueueLength() {
        synchronized (_lock) {
            return _queueLength;
        }
    }

    /**
     * Safely starts this TCP connection after event handlers have been set.
     */
    public void start() {
        synchronized(_lock) {
            if (_shutdown || _started)
                return;

            _started = true;

            // kick off after a random amount of time to avoid resource usage spikes

            int kickoffTime = 1000 + s_random.nextInt(KICKOFF_DELAY);

            _startTimer = _timerThread.schedule(_threadPool, new TimerTask() {

                @Override
                public void run() {
                    connect();
                }

            }, kickoffTime);
        }
    }

    /**
     * Establishes a socket and continually reads.
     */
    private void connect() {
        if (_shutdown)
            return;

        // since thread-pool need this every time
        _threadStateHandler.handle();

        WebSocketClient webSocket = null;

        try {
            webSocket = new WebSocketClient(URI.create(_dest), new Draft_6455(), _headers, _timeout) {

                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    _counterConnections.incr();

                    // update flag
                    _lastSuccessfulConnection = System.nanoTime();

                    synchronized (_lock) {
                        if (_shutdown)
                            return;

                        _webSocket = this;

                        // connection has been successful so reset variables
                        // related to exponential back-off

                        _recentlyConnected = true;

                        _backoffTime = MIN_CONNETION_GAP;
                    }

                    // set receive timeout
                    try {
                        this.getSocket().setSoTimeout(_timeout);
                    } catch (IOException exc) {
                        onError(exc);
                        return;
                    }

                    // fire the connected event
                    _threadStateHandler.handle();
                    _callbackHandler.handle(_connectedCallback, _callbackErrorHandler);
                }

                @Override
                public void onMessage(String message) {
                    _threadStateHandler.handle();
                    handleReceivedData(message);
                }

                @Override
                public void onMessage(ByteBuffer rawMessage) {
                    // keep each byte as 8-bit character (ISO-8859-1 keeps chars as 8-bits)
                    _threadStateHandler.handle();
                    handleReceivedData(StandardCharsets.ISO_8859_1.decode(rawMessage).toString());
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    Handler.tryHandle(_disconnectedCallback, _callbackErrorHandler);
                }

                @Override
                public void onError(Exception ex) {
                    Handler.tryHandle(_disconnectedCallback, _callbackErrorHandler);
                }

            };
            webSocket.connect();

            synchronized (_lock) {
                _webSocket = webSocket;
            }

        } catch (Exception exc) {
            safeClose(webSocket);

            onError();
        }
    }

    private void onError() {
        if (_shutdown) {
            // thread can gracefully exit
            return;
        }

        if (_recentlyConnected)
            _backoffTime = MIN_CONNETION_GAP;

        else {
            _backoffTime = Math.min(_backoffTime * 2, MAX_BACKOFF);
            _backoffTime = Math.max(MIN_CONNETION_GAP, _backoffTime);
        }

        long timeDiff = (System.nanoTime() - _lastSuccessfulConnection) / 1000000;
        if (timeDiff > _timeout) {
            // reset the timestamp
            _lastSuccessfulConnection = System.nanoTime();

            _callbackHandler.handle(_timeoutCallback, _callbackErrorHandler);
        }

        _recentlyConnected = false;

        _startTimer = _timerThread.schedule(_threadPool, new TimerTask() {

            @Override
            public void run() {
                connect();
            }

        }, _backoffTime);
    }

    /**
     * When an actual data segment was received; deals with request callbacks if necessary
     */
    private void handleReceivedData(String data) {
        // deal with any queued callbacks first

        QueuedRequest request = null;

        // then check for any requests
        // (should release lock as soon as possible)
        synchronized (_lock) {
            if (_activeRequest != null) {
                request = _activeRequest;
                _activeRequest = null;
            }
        }

        if (request != null && request.timeout > 0) {
            // make sure it hasn't been too long i.e. timeout
            if (request.isExpired()) {
                _logger.debug("Active request has expired");

                // fire the timeout handler
                _callbackHandler.handle(_timeoutCallback, _callbackErrorHandler);
            } else {
                // fire the response request's response handler
                request.setResponse(data);

                _callbackHandler.handle(request.responseHandler, data, _callbackErrorHandler);
            }
        }

        // ...then fire the 'received' callback next
        _callbackHandler.handle(_receivedCallback, data, _callbackErrorHandler);

        processQueue();
    }

    private class QueuedRequest {

        /**
         * The buffer that's already been validated i.e. not empty and UTF8 encoded
         * (or null if send data wasn't used in the first place)
         */
        public byte[] requestBuffer;

        /**
         * The original request/send data (only need 'sent' callback)
         * (can be null)
         */
        public String request;

        /**
         * For long term expiry detection.
         *
         * (based on 'nanoTime')
         */
        public long timeQueued = System.nanoTime();

        /**
         * (millis)
         */
        public int timeout;

        /**
         * (based on System.nanoTime)
         */
        public long timeStarted;

        /**
         * The (optional) callback
         */
        public H1<String> responseHandler;

        /**
         * Stores the response itself (for synchronous operation)
         */
        public String response;

        public QueuedRequest(byte[] requestBuffer, String origData, int timeout, H1<String> responseHandler) {
            this.requestBuffer = requestBuffer;
            this.request = origData;
            this.timeout = timeout;
            this.responseHandler = responseHandler;
        }

        /**
         * Sets response value and fire any callbacks
         */
        public void setResponse(String data) {
            synchronized (this) {
                this.response = data;

                // (for synchronous responses)
                this.notify();
            }
        }

        /**
         * Starts the timeout timer.
         */
        public void startTimeout() {
            this.timeStarted = System.nanoTime();
        }

        /**
         * Checks whether request has expired.
         */
        public boolean isExpired() {
            long timeDiff = (System.nanoTime() - this.timeStarted) / 1000000L;
            return timeDiff > this.timeout;
        }

        /**
         * For detecting long term delayed requests.
         */
        public boolean isLongTermExpired() {
            long timeDiff = (System.nanoTime() - this.timeQueued) / 1000000L;
            return timeDiff > _longTermRequestTimeout;
        }
    }

    /**
     * For complete control of a request
     */
    public void queueRequest(String requestData, int timeout, H1<String> responseHandler) {
        byte[] buffer = stringToBuffer(requestData);

        // buffer can be null, which means a 'send' is not necessary, but a response is

        QueuedRequest request = new QueuedRequest(buffer, requestData, timeout, responseHandler);

        doQueueRequest(request);
    }

    public void doQueueRequest(QueuedRequest request) {
        // whether or not this entry had to be queued
        boolean queued = false;

        synchronized (_lock) {
            if (_activeRequest == null) {
                _logger.debug("Active request made. data:[{}]", request.request);

                // make it the active request and don't queue it
                _activeRequest = request;

                // will be sent next, so start timing
                _activeRequest.startTimeout();

            } else {
                _logger.debug("Queued a request. data:[{}]", request.request);

                // a request is active, so queue this new one
                _requestQueue.add(request);
                _queueLength++;
                queued = true;
            }
        }

        if (!queued && request.requestBuffer != null) {
            sendBufferNow(request.requestBuffer, request.request, true);
        }

        // without a timer, the queue needs to serviced on both send and receive
        processQueue();
    }

    /**
     * (Overloaded) (uses default timeout value)
     */
    public void request(String requestData, H1<String> responseHandler) {
        // don't bother doing anything if empty or missing
        if (Strings.isEmpty(requestData))
            return;

        queueRequest(requestData, _requestTimeout, responseHandler);
    }

    /**
     * (synchronous version)
     */
    public String requestWaitAndReceive(String requestData) {
        int recvTimeout = getRequestTimeout();

        final String[] response = new String[1];

        synchronized (response) {
            queueRequest(requestData, recvTimeout, new H1<String>() {

                @Override
                public void handle(String value) {
                    synchronized (response) {
                        response[0] = value;

                        response.notify();
                    }
                }

            });

            // wait for a while or until notified when a response is received
            Threads.waitOnSync(response, recvTimeout);
        }

        return response[0];
    }

    /**
     * Receives data and invokes a callback
     */
    public void receive(H1<String> responseHandler) {
        queueRequest(null, _requestTimeout, responseHandler);
    }

    /**
     * Synchronous version.
     */
    public String waitAndReceive() {
        int recvTimeout = getRequestTimeout();
        QueuedRequest request = new QueuedRequest(null, null, recvTimeout, null);

        synchronized(request) {
            queueRequest(null, _requestTimeout, null);

            // wait for a while or until notified when a response is received
            Threads.waitOnSync(request, recvTimeout);
        }

        return request.response;
    }

    /**
     * Clears the active request and any queue requests.
     */
    public void clearQueue() {
        synchronized (_lock) {
            boolean activeRequestCleared = false;

            // clear the active request
            if(_activeRequest != null){
                _activeRequest = null;
                activeRequestCleared = true;
            }

            int count = 0;

            // clear the queue
            while (_requestQueue.poll() != null) {
                count++;
                _queueLength--;
            }

            _logger.debug("Cleared queue. activeRequest={}, queueCount={}", activeRequestCleared, count);
        }
    }

    /**
     * Expires and initiates requests in the queue
     * (assumes not synced)
     */
    private void processQueue() {
        // if any new requests are found
        QueuedRequest nextRequest = null;

        // if a timeout callback needs to be fired
        boolean callTimeout = false;

        synchronized (_lock) {
            // check if any active requests need expiring
            if (_activeRequest != null) {
                if (_activeRequest.responseHandler == null || _activeRequest.timeout <= 0) {
                    // was a blind request or
                    _activeRequest = null;

                } else if (_activeRequest.isExpired()) {
                    _logger.debug("Active request has expired");

                    // timeout callback must be fired
                    callTimeout = true;

                    // clear active request
                    _activeRequest = null;

                } else if (_activeRequest.isLongTermExpired()) {
                    _logger.debug("Active request has long term expired");

                    callTimeout = true;

                    _activeRequest = null;
                } else {
                    // there is still an valid active request,
                    // so just get out of here
                    return;
                }
            }
        }

        // call the timeout callback, (there's an opportunity for request queue to be cleared by callback handler)
        if (callTimeout)
            _callbackHandler.handle(_timeoutCallback, _callbackErrorHandler);

        // an active request might have come in
        synchronized (_lock) {
            if (_activeRequest != null) {
                // there's an active request, so leave it to play out
                return;
            }
            // record this for logging
            int longTermDropped = 0;

            try {
                for (;;) {
                    // no active request, check for queued ones
                    nextRequest = _requestQueue.poll();

                    if (nextRequest == null) {
                        // no more requests either, so nothing more to do
                        _logger.debug("No new requests in queue.");

                        return;
                    }

                    _queueLength--;

                    if (!nextRequest.isLongTermExpired())
                        break;

                    // otherwise, continue to expire the long term queued
                    longTermDropped++;
                }
            } finally {
                if (longTermDropped > 0)
                    _logger.debug("(dropped {} long term queued requests.)", longTermDropped);
            }

            // set active request and start timeout *before* sending
            _activeRequest = nextRequest;
            nextRequest.startTimeout();
        }

        // if the request has send data 'data' send now
        if (nextRequest.requestBuffer != null)
            sendBufferNow(nextRequest.requestBuffer, nextRequest.request, false);
    }

    /**
     * Safely sends data without overlapping any existing requests
     */
    public void send(String data) {
        byte[] buffer = stringToBuffer(data);

        QueuedRequest request = new QueuedRequest(buffer, data, 0, null);

        doQueueRequest(request);
    }

    /**
     * Sends data. Returns immediately. Will not throw any exceptions.
     */
    public void sendNow(final String data) {
        byte[] buffer = stringToBuffer(data);

        if (buffer == null)
            return;

        sendBufferNow(buffer, data, true);
    }

    /**
     * Sends a prepared buffer immediately, optionally using a thread-pool
     */
    private void sendBufferNow(final byte[] buffer, final String origData, boolean onThreadPool) {
        final WebSocket webSocket;

        synchronized(_lock) {
            webSocket = _webSocket;
        }

        if (webSocket != null) {
            if (onThreadPool) {
                _threadPool.execute(new Runnable() {

                    @Override
                    public void run() {
                        _threadStateHandler.handle();
                        sendBufferNow0(_webSocket, buffer, origData);
                    }

                });
            } else {
                sendBufferNow0(_webSocket, buffer, origData);
            }
        }
    }

    /**
     * (convenience method)
     */
    private void sendBufferNow0(WebSocket webSocket, byte[] buffer, String origData) {
        try {
            webSocket.send(origData);
        } catch (Exception exc) {
            // ignore
        }

        Handler.tryHandle(_sentCallback, origData, _callbackErrorHandler);
    }

    /**
     * Converts a direct char-to-byte conversion, and includes a suffix
     */
    private static byte[] stringToBuffer(String str) {
        int strLen = (str != null ? str.length() : 0);

        byte[] buffer = new byte[strLen];
        for (int a = 0; a < strLen; a++)
            buffer[a] = (byte) (str.charAt(a) & 0xff);

        return buffer;
    }

    /**
     * Raw buffer to string.
     */
    private String bufferToString(byte[] buffer, int offset, int len) {
        char[] cBuffer = new char[len];
        for (int a=0; a<len; a++) {
            cBuffer[a] = (char) (buffer[offset + a] & 0xff);
        }

        return new String(cBuffer);
    }


    /**
     * Drops this socket; may trigger a reconnect.
     */
    public void drop() {
        synchronized (_lock) {
            if (_shutdown)
                return;

            safeClose(_webSocket);
        }
    }

    /**
     * Permanently shuts down this managed TCP connection.
     */
    @Override
    public void close() {
        synchronized(_lock) {
            if (_shutdown)
                return;

            _shutdown = true;

            if (_startTimer != null)
                _startTimer.cancel();

            safeClose(_webSocket);

            _webSocket = null;

            // notify the connection and receive thread if it happens to be waiting
            _lock.notify();
        }
    }

    private void safeClose(WebSocketClient socket) {
        if (socket == null)
            return;

        socket.close();
    }

}
