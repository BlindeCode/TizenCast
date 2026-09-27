/*
 * TizenCast service - FCast receiver backend for TizenBrew.
 *
 * TizenBrew downloads this file and runs it with `vm.runInContext` inside its
 * own Node.js service. That environment dictates how this file is written:
 *   - Only Node built-in modules can be required (no `ws`, no npm packages).
 *   - Older TVs run Node v4.4.3, so the code is plain ES5 and avoids
 *     Buffer.from/Buffer.alloc when they are not available.
 *   - An uncaught exception would take down TizenBrew's whole service, so
 *     every callback is wrapped in `guard`.
 *
 * Ports (same as the official FCast receivers):
 *   46899/tcp  FCast over TCP                          (LAN)
 *   46898/tcp  FCast over WebSocket                    (LAN)
 *   46900/tcp  Receiver page bridge (WebSocket at /ui), media proxy
 *              (/proxy/...) and manifest store (/content/...)  (loopback only)
 *   5353/udp   mDNS advertisement of _fcast._tcp and _fcast-ws._tcp
 */
(function () {
    'use strict';

    var net = require('net');
    var http = require('http');
    var https = require('https');
    var crypto = require('crypto');
    var dgram = require('dgram');
    var os = require('os');
    var url = require('url');
    var fs = require('fs');

    var VERSION = '0.7.0';
    var TCP_PORT = 46899;
    var WS_PORT = 46898;
    var UI_PORT = 46900;
    var UI_HOST = '127.0.0.1';
    var UI_ORIGIN = 'http://' + UI_HOST + ':' + UI_PORT;

    // The FCast spec limits packets to 32 KB. Be lenient with senders that
    // exceed it (large DASH manifests), but keep a hard cap.
    var MAX_PACKET_LENGTH = 1024 * 1024;
    var TCP_HEARTBEAT_TIMEOUT = 2500;
    var TCP_HEARTBEAT_RETRIES = 3;
    var PENDING_PLAY_TTL = 60000;
    var APP_LAUNCH_THROTTLE = 10000;
    var TIZENBREW_CONFIG = '/home/owner/share/tizenbrewConfig.json';
    var WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    var Opcode = {
        None: 0,
        Play: 1,
        Pause: 2,
        Resume: 3,
        Stop: 4,
        Seek: 5,
        PlaybackUpdate: 6,
        VolumeUpdate: 7,
        SetVolume: 8,
        PlaybackError: 9,
        SetSpeed: 10,
        Version: 11,
        Ping: 12,
        Pong: 13
    };

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    function log() {
        var args = Array.prototype.slice.call(arguments);
        args.unshift('[TizenCast]');
        try { console.log.apply(console, args); } catch (e) { /* ignore */ }
    }

    function errorText(e) {
        if (!e) return 'unknown error';
        return e.stack || e.message || String(e);
    }

    // Wraps a callback so that an exception never escapes into TizenBrew.
    function guard(fn, onError) {
        return function () {
            try {
                return fn.apply(this, arguments);
            } catch (e) {
                log('Unhandled error:', errorText(e));
                if (onError) {
                    try { onError(e); } catch (e2) { /* ignore */ }
                }
            }
        };
    }

    // Buffer.from/alloc only exist from Node 4.5 / 5.10 onwards. Note that
    // `Buffer.from` alone is not a usable check on Node 4.4, where it is
    // inherited from Uint8Array.
    var NEW_BUFFER_API = typeof Buffer.alloc === 'function' && typeof Buffer.from === 'function';

    function bufAlloc(size) {
        if (NEW_BUFFER_API) return Buffer.alloc(size);
        var b = new Buffer(size);
        b.fill(0);
        return b;
    }

    function bufFrom(value, encoding) {
        return NEW_BUFFER_API ? Buffer.from(value, encoding) : new Buffer(value, encoding);
    }

    function uint16(value) {
        var b = bufAlloc(2);
        b.writeUInt16BE(value & 0xffff, 0);
        return b;
    }

    function toNumber(value, fallback) {
        var n = Number(value);
        return isFinite(n) ? n : fallback;
    }

    // Timers are tracked so that a restarted instance can clean up after the
    // previous one (TizenBrew re-runs this file after a crash).
    var timers = [];

    function later(fn, ms) {
        var t = setTimeout(function () {
            var i = timers.indexOf(t);
            if (i >= 0) timers.splice(i, 1);
            guard(fn)();
        }, ms);
        timers.push(t);
        return t;
    }

    function every(fn, ms) {
        var t = setInterval(guard(fn), ms);
        timers.push(t);
        return t;
    }

    // ---------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------

    var stopped = false;
    var servers = [];
    var sockets = [];

    var previous = process.__tizenCast;
    if (previous && typeof previous.shutdown === 'function') {
        log('Stopping previous instance');
        try { previous.shutdown(); } catch (e) { log('Previous instance shutdown failed:', errorText(e)); }
    }
    process.__tizenCast = { version: VERSION, shutdown: shutdown };

    function trackSocket(socket) {
        sockets.push(socket);
        socket.on('close', function () {
            var i = sockets.indexOf(socket);
            if (i >= 0) sockets.splice(i, 1);
        });
    }

    function shutdown() {
        if (stopped) return;
        stopped = true;
        timers.forEach(function (t) { clearTimeout(t); clearInterval(t); });
        timers = [];
        try { mdnsGoodbye(); } catch (e) { /* ignore */ }
        if (mdns.socket) {
            try { mdns.socket.close(); } catch (e) { /* ignore */ }
            mdns.socket = null;
        }
        servers.forEach(function (s) { try { s.close(); } catch (e) { /* ignore */ } });
        sockets.slice().forEach(function (s) { try { s.destroy(); } catch (e) { /* ignore */ } });
        servers = [];
        sockets = [];
    }

    function listen(server, port, host, name) {
        var attempts = 0;
        servers.push(server);

        function tryListen() {
            if (stopped) return;
            attempts++;
            if (host) server.listen(port, host);
            else server.listen(port);
        }

        server.on('error', function (e) {
            if (e && e.code === 'EADDRINUSE' && attempts < 15 && !stopped) {
                log(name + ': port ' + port + ' in use, retrying');
                later(tryListen, 2000);
                return;
            }
            log(name + ' error:', errorText(e));
            sendToUi('toast', { message: name + ' unavailable: ' + (e && e.message ? e.message : e), error: true });
        });
        server.on('listening', function () {
            log(name + ' listening on port ' + port);
        });
        tryListen();
    }

    // ---------------------------------------------------------------------
    // Device information
    // ---------------------------------------------------------------------

    var device = {
        name: 'TizenCast',
        id: null,
        fallbackAddress: null
    };

    function getIPv4Addresses() {
        var result = [];
        var linkLocal = [];
        var ifaces = {};
        try { ifaces = os.networkInterfaces() || {}; } catch (e) { /* ignore */ }
        Object.keys(ifaces).forEach(function (name) {
            (ifaces[name] || []).forEach(function (a) {
                if (!a || a.internal) return;
                if (a.family !== 'IPv4' && a.family !== 4) return;
                if (result.indexOf(a.address) >= 0 || linkLocal.indexOf(a.address) >= 0) return;
                if (a.address.indexOf('169.254.') === 0) linkLocal.push(a.address);
                else result.push(a.address);
            });
        });
        if (result.length === 0 && device.fallbackAddress) result.push(device.fallbackAddress);
        return result.length ? result : linkLocal;
    }

    function getTizenCapability(key) {
        try {
            if (typeof tizen !== 'undefined' && tizen.systeminfo) {
                return tizen.systeminfo.getCapability(key);
            }
        } catch (e) { /* ignore */ }
        return null;
    }

    function getDeviceInfo() {
        return {
            name: device.name,
            addresses: getIPv4Addresses(),
            ports: { tcp: TCP_PORT, ws: WS_PORT },
            discovery: !!mdns.socket,
            version: VERSION
        };
    }

    // The TV's own remote control API exposes its user-facing name
    // ("[TV] Samsung 7 Series (55)"), a unique id and its IP address.
    function loadDeviceInfo(callback) {
        var done = false;
        var model = getTizenCapability('http://tizen.org/system/model_name');
        var tizenId = getTizenCapability('http://tizen.org/system/tizenid');

        function finish(info) {
            if (done) return;
            done = true;
            var tvName = info && (info.device && info.device.name || info.name);
            var tvModel = info && info.device && info.device.modelName;
            var label = tvName ? String(tvName).replace(/^\[TV\]\s*/, '') : (tvModel || model);
            device.name = label ? 'TizenCast - ' + label : 'TizenCast';
            device.id = (info && info.device && (info.device.duid || info.device.id)) || tizenId || null;
            if (info && info.device && info.device.ip) device.fallbackAddress = String(info.device.ip);
            callback();
        }

        try {
            var req = http.get({ host: '127.0.0.1', port: 8001, path: '/api/v2/' }, function (res) {
                var chunks = [];
                res.on('data', function (c) { chunks.push(c); });
                res.on('end', guard(function () {
                    var info = null;
                    try { info = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch (e) { /* ignore */ }
                    finish(info);
                }, function () { finish(null); }));
                res.on('error', function () { finish(null); });
            });
            req.on('error', function () { finish(null); });
            req.setTimeout(2500, function () { req.abort(); finish(null); });
        } catch (e) {
            finish(null);
        }
    }

    // ---------------------------------------------------------------------
    // Minimal RFC 6455 WebSocket server connection
    // ---------------------------------------------------------------------

    function WebSocketConnection(socket, maxMessageSize) {
        var self = this;
        this.socket = socket;
        this.maxMessageSize = maxMessageSize;
        this.buffer = null;
        this.fragments = null;
        this.fragmentOpcode = 0;
        this.fragmentSize = 0;
        this.closed = false;
        this.onmessage = null;
        this.onclose = null;

        socket.setNoDelay(true);
        socket.on('data', guard(function (chunk) {
            self._onData(chunk);
        }, function () {
            self.destroy();
        }));
        socket.on('error', function (e) {
            log('WebSocket error:', e && e.message);
            self.destroy();
        });
        socket.on('close', function () {
            self._onClosed();
        });
    }

    WebSocketConnection.accept = function (req, socket, head, maxMessageSize) {
        var key = req.headers['sec-websocket-key'];
        var upgrade = String(req.headers.upgrade || '').toLowerCase();
        if (!key || upgrade !== 'websocket') {
            socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
            return null;
        }

        var accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
        var response = [
            'HTTP/1.1 101 Switching Protocols',
            'Upgrade: websocket',
            'Connection: Upgrade',
            'Sec-WebSocket-Accept: ' + accept
        ];
        var protocols = req.headers['sec-websocket-protocol'];
        if (protocols) response.push('Sec-WebSocket-Protocol: ' + String(protocols).split(',')[0].trim());
        socket.write(response.join('\r\n') + '\r\n\r\n');

        // The HTTP server's idle timeout (120s on older Node) must not close
        // long-lived WebSocket connections.
        socket.setTimeout(0);
        var ws = new WebSocketConnection(socket, maxMessageSize);
        if (head && head.length) {
            // Delivered after the caller has attached its handlers.
            process.nextTick(guard(function () { ws._onData(head); }, function () { ws.destroy(); }));
        }
        return ws;
    };

    WebSocketConnection.prototype._onData = function (chunk) {
        this.buffer = this.buffer && this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;

        while (!this.closed && this.buffer.length >= 2) {
            var buf = this.buffer;
            var fin = (buf[0] & 0x80) !== 0;
            var opcode = buf[0] & 0x0f;
            var masked = (buf[1] & 0x80) !== 0;
            var length = buf[1] & 0x7f;
            var offset = 2;

            if (length === 126) {
                if (buf.length < 4) return;
                length = buf.readUInt16BE(2);
                offset = 4;
            } else if (length === 127) {
                if (buf.length < 10) return;
                if (buf.readUInt32BE(2) !== 0) return this.close(1009);
                length = buf.readUInt32BE(6);
                offset = 10;
            }

            if (length > this.maxMessageSize) return this.close(1009);

            var mask = null;
            if (masked) {
                if (buf.length < offset + 4) return;
                mask = buf.slice(offset, offset + 4);
                offset += 4;
            }
            if (buf.length < offset + length) return;

            var payload = buf.slice(offset, offset + length);
            if (mask) {
                for (var i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
            }
            this.buffer = buf.slice(offset + length);
            this._onFrame(fin, opcode, payload);
        }
    };

    WebSocketConnection.prototype._onFrame = function (fin, opcode, payload) {
        switch (opcode) {
            case 0x0: // continuation
                if (!this.fragments) return this.close(1002);
                this.fragments.push(payload);
                this.fragmentSize += payload.length;
                if (this.fragmentSize > this.maxMessageSize) return this.close(1009);
                if (fin) {
                    var message = Buffer.concat(this.fragments);
                    var binary = this.fragmentOpcode === 0x2;
                    this.fragments = null;
                    this._emitMessage(message, binary);
                }
                break;
            case 0x1: // text
            case 0x2: // binary
                if (fin) {
                    this._emitMessage(payload, opcode === 0x2);
                } else {
                    this.fragments = [payload];
                    this.fragmentOpcode = opcode;
                    this.fragmentSize = payload.length;
                }
                break;
            case 0x8: // close
                var code = payload.length >= 2 ? payload.readUInt16BE(0) : 1000;
                this.close(code === 1005 || code === 1006 ? 1000 : code);
                break;
            case 0x9: // ping
                this._sendFrame(0xA, payload);
                break;
            case 0xA: // pong
                break;
            default:
                this.close(1002);
        }
    };

    WebSocketConnection.prototype._emitMessage = function (data, binary) {
        if (this.onmessage) this.onmessage(data, binary);
    };

    WebSocketConnection.prototype._sendFrame = function (opcode, payload) {
        if (this.closed) return;
        var length = payload.length;
        var header;
        if (length < 126) {
            header = bufAlloc(2);
            header[1] = length;
        } else if (length < 65536) {
            header = bufAlloc(4);
            header[1] = 126;
            header.writeUInt16BE(length, 2);
        } else {
            header = bufAlloc(10);
            header[1] = 127;
            header.writeUInt32BE(0, 2);
            header.writeUInt32BE(length, 6);
        }
        header[0] = 0x80 | opcode;
        this.socket.write(Buffer.concat([header, payload]));
    };

    WebSocketConnection.prototype.sendText = function (text) {
        this._sendFrame(0x1, bufFrom(text, 'utf8'));
    };

    WebSocketConnection.prototype.sendBinary = function (data) {
        this._sendFrame(0x2, data);
    };

    WebSocketConnection.prototype.close = function (code) {
        if (this.closed) return;
        try {
            this._sendFrame(0x8, uint16(code || 1000));
        } catch (e) { /* ignore */ }
        this.closed = true;
        try { this.socket.end(); } catch (e) { /* ignore */ }
        this._onClosed();
    };

    WebSocketConnection.prototype.destroy = function () {
        this.closed = true;
        try { this.socket.destroy(); } catch (e) { /* ignore */ }
        this._onClosed();
    };

    WebSocketConnection.prototype._onClosed = function () {
        this.closed = true;
        if (this.onclose) {
            var cb = this.onclose;
            this.onclose = null;
            cb();
        }
    };

    // ---------------------------------------------------------------------
    // FCast sessions
    // ---------------------------------------------------------------------

    var sessions = [];
    var nextSessionId = 1;

    function FCastSession(type, address, write, close) {
        this.id = type + '-' + (nextSessionId++);
        this.type = type;
        this.address = address;
        this.write = write;
        this.closeTransport = close;
        this.buffer = null;
        this.closed = false;
    }

    FCastSession.prototype.send = function (opcode, message) {
        if (this.closed) return;
        var body = message === undefined || message === null ? null : bufFrom(JSON.stringify(message), 'utf8');
        var header = bufAlloc(5);
        header.writeUInt32LE(1 + (body ? body.length : 0), 0);
        header[4] = opcode;
        try {
            this.write(body ? Buffer.concat([header, body]) : header);
        } catch (e) {
            log('Failed to send to ' + this.id + ':', e && e.message);
            this.close();
        }
    };

    FCastSession.prototype.close = function () {
        if (this.closed) return;
        this.closed = true;
        try { this.closeTransport(); } catch (e) { /* ignore */ }
    };

    // Packets are [uint32 LE length][uint8 opcode][UTF-8 JSON body], where
    // length covers the opcode and body. TCP delivers them as a byte stream.
    FCastSession.prototype.processBytes = function (chunk) {
        this.buffer = this.buffer && this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;

        while (!this.closed && this.buffer.length >= 4) {
            var length = this.buffer.readUInt32LE(0);
            if (length < 1 || length > MAX_PACKET_LENGTH) {
                log('Invalid packet length ' + length + ' from ' + this.id + ', disconnecting');
                this.close();
                return;
            }
            if (this.buffer.length < 4 + length) return;

            var opcode = this.buffer[4];
            var body = length > 1 ? this.buffer.toString('utf8', 5, 4 + length) : null;
            this.buffer = this.buffer.slice(4 + length);
            this.handlePacket(opcode, body);
        }
    };

    FCastSession.prototype.handlePacket = function (opcode, body) {
        var message = null;
        if (body) {
            try {
                message = JSON.parse(body);
            } catch (e) {
                log('Ignoring packet with invalid JSON (opcode ' + opcode + ')');
                return;
            }
        }

        switch (opcode) {
            case Opcode.Play:
                if (message) onPlay(message);
                break;
            case Opcode.Pause:
                sendToUi('pause');
                break;
            case Opcode.Resume:
                sendToUi('resume');
                break;
            case Opcode.Stop:
                pendingPlay = null;
                sendToUi('stop');
                break;
            case Opcode.Seek:
                if (message) sendToUi('seek', { time: toNumber(message.time, 0) });
                break;
            case Opcode.SetVolume:
                if (message) sendToUi('setvolume', { volume: Math.max(0, Math.min(1, toNumber(message.volume, 1))) });
                break;
            case Opcode.SetSpeed:
                if (message) sendToUi('setspeed', { speed: toNumber(message.speed, 1) });
                break;
            case Opcode.Version:
                log(this.id + ' uses protocol version', message && message.version);
                break;
            case Opcode.Ping:
                this.send(Opcode.Pong);
                break;
            case Opcode.Pong:
            case Opcode.PlaybackUpdate:
            case Opcode.VolumeUpdate:
            case Opcode.PlaybackError:
                break;
            default:
                log('Ignoring unsupported opcode ' + opcode + ' from ' + this.id);
                break;
        }
    };

    function formatAddress(socket) {
        return String(socket.remoteAddress || '').replace(/^::ffff:/, '') + ':' + socket.remotePort;
    }

    function addSession(session) {
        sessions.push(session);
        log('Sender connected: ' + session.id + ' (' + session.address + ')');
        sendToUi('connect', { id: session.id, type: session.type, address: session.address, count: sessions.length });
        session.send(Opcode.Version, { version: 2 });
    }

    function removeSession(session) {
        var i = sessions.indexOf(session);
        if (i < 0) return;
        sessions.splice(i, 1);
        log('Sender disconnected: ' + session.id);
        sendToUi('disconnect', { id: session.id, type: session.type, address: session.address, count: sessions.length });
    }

    function broadcast(opcode, message) {
        sessions.slice().forEach(function (s) { s.send(opcode, message); });
    }

    // FCast over TCP ------------------------------------------------------

    function startTcpListener() {
        var server = net.createServer(guard(function (socket) {
            trackSocket(socket);
            var address = formatAddress(socket);
            var heartbeatRetries = 0;
            var session = new FCastSession('tcp', address, function (data) {
                socket.write(data);
            }, function () {
                socket.destroy();
            });

            socket.setNoDelay(true);
            socket.setTimeout(TCP_HEARTBEAT_TIMEOUT);
            socket.on('timeout', guard(function () {
                if (heartbeatRetries >= TCP_HEARTBEAT_RETRIES) {
                    log('No response from ' + address + ', disconnecting');
                    session.close();
                    return;
                }
                heartbeatRetries++;
                session.send(Opcode.Ping);
            }));
            socket.on('data', guard(function (data) {
                heartbeatRetries = 0;
                session.processBytes(data);
            }, function () {
                session.close();
            }));
            socket.on('error', function (e) {
                log('TCP error from ' + address + ':', e && e.message);
                session.close();
            });
            socket.on('close', function () {
                session.closed = true;
                removeSession(session);
            });

            addSession(session);
        }));
        listen(server, TCP_PORT, null, 'FCast TCP listener');
    }

    // FCast over WebSocket ------------------------------------------------

    function startWebSocketListener() {
        var server = http.createServer(function (req, res) {
            res.writeHead(426, { 'Content-Type': 'text/plain', 'Upgrade': 'websocket' });
            res.end('FCast WebSocket endpoint');
        });
        server.on('clientError', function (e, socket) {
            try { socket.destroy(); } catch (x) { /* ignore */ }
        });
        server.on('connection', trackSocket);
        server.on('upgrade', guard(function (req, socket, head) {
            var ws = WebSocketConnection.accept(req, socket, head, 2 * (MAX_PACKET_LENGTH + 4));
            if (!ws) return;

            var address = formatAddress(socket);
            var session = new FCastSession('ws', address, function (data) {
                ws.sendBinary(data);
            }, function () {
                ws.close(1000);
            });

            ws.onmessage = guard(function (data, binary) {
                if (!binary) {
                    log('Ignoring text frame from ' + address);
                    return;
                }
                session.processBytes(data);
            }, function () {
                session.close();
            });
            ws.onclose = function () {
                session.closed = true;
                removeSession(session);
            };

            addSession(session);
        }));
        listen(server, WS_PORT, null, 'FCast WebSocket listener');
    }

    // ---------------------------------------------------------------------
    // Receiver page bridge
    // ---------------------------------------------------------------------

    var ui = null;              // { ws, visible }
    var pendingPlay = null;     // { message, at }
    var moduleFullName = null;  // e.g. "gh/BlindeCode/TizenCast" or "npm/tizencast"
    var lastLaunchAt = 0;
    var proxyHeaders = null;
    var lastProxyOrigin = null;
    var contentStore = {};
    var contentCounter = 0;

    function sendToUi(type, data) {
        if (!ui) return;
        try {
            ui.ws.sendText(JSON.stringify({ type: type, data: data === undefined ? null : data }));
        } catch (e) {
            log('Failed to send to receiver page:', e && e.message);
        }
    }

    function contentExtension(container) {
        var c = String(container || '').toLowerCase();
        if (c.indexOf('dash') >= 0) return '.mpd';
        if (c.indexOf('mpegurl') >= 0) return '.m3u8';
        return '';
    }

    function onPlay(message) {
        if (typeof message.url !== 'string' && typeof message.content !== 'string') {
            log('Ignoring play message without url or content');
            return;
        }

        var play = {
            container: typeof message.container === 'string' ? message.container : '',
            url: typeof message.url === 'string' ? message.url : null,
            content: typeof message.content === 'string' ? message.content : null,
            time: message.time === undefined || message.time === null ? null : toNumber(message.time, null),
            speed: message.speed === undefined || message.speed === null ? null : toNumber(message.speed, null),
            headers: message.headers && typeof message.headers === 'object' ? message.headers : null
        };

        // Custom headers are applied by the media proxy, which the receiver
        // page uses for streams it has to fetch itself.
        proxyHeaders = play.headers;

        if (play.content) {
            // Serve inline manifests over HTTP so players can load them.
            var id = ++contentCounter;
            contentStore = {};
            contentStore[id] = { body: play.content, type: play.container || 'application/octet-stream' };
            play.contentUrl = UI_ORIGIN + '/content/' + id + contentExtension(play.container);
        }

        log('Play requested:', play.container, play.url || '(inline content)');
        pendingPlay = { message: play, at: Date.now() };
        if (!deliverPendingPlay()) {
            launchReceiverPage();
        }
    }

    function deliverPendingPlay() {
        if (!pendingPlay) return false;
        if (Date.now() - pendingPlay.at > PENDING_PLAY_TTL) {
            pendingPlay = null;
            return false;
        }
        if (!ui || !ui.visible) return false;
        sendToUi('play', pendingPlay.message);
        pendingPlay = null;
        return true;
    }

    function isModuleName(name) {
        return typeof name === 'string' && /^[a-z]+\/[^\s]+$/i.test(name);
    }

    function findModuleInConfig() {
        try {
            var config = JSON.parse(fs.readFileSync(TIZENBREW_CONFIG, 'utf8'));
            var modules = config && config.modules || [];
            for (var i = 0; i < modules.length; i++) {
                if (/tizencast/i.test(modules[i])) return modules[i];
            }
        } catch (e) { /* ignore */ }
        return null;
    }

    // Brings the receiver page to the foreground through TizenBrew's app
    // control entry point (the same mechanism TizenTube uses for DIAL).
    function launchReceiverPage() {
        var now = Date.now();
        if (now - lastLaunchAt < APP_LAUNCH_THROTTLE) return;

        var fullName = moduleFullName || findModuleInConfig();
        if (!fullName) {
            log('Cannot open the receiver page: module name unknown. Open TizenCast from TizenBrew once.');
            return;
        }

        try {
            if (typeof tizen === 'undefined' || !tizen.application) {
                log('Cannot open the receiver page: Tizen application API unavailable');
                return;
            }
            lastLaunchAt = now;
            var slash = fullName.indexOf('/');
            var data = JSON.stringify({
                moduleType: fullName.substring(0, slash),
                moduleName: fullName.substring(slash + 1),
                args: 'source=fcast'
            });
            var appId = tizen.application.getAppInfo().packageId + '.TizenBrewStandalone';
            var control = new tizen.ApplicationControl(
                'http://tizen.org/appcontrol/operation/view', null, null, null,
                [new tizen.ApplicationControlData('module', [data])]
            );
            log('Opening receiver page via ' + appId);
            tizen.application.launchAppControl(control, appId, function () {
                log('TizenBrew launched');
            }, function (e) {
                log('Failed to launch TizenBrew:', e && e.message);
            });
        } catch (e) {
            log('Failed to launch TizenBrew:', errorText(e));
        }
    }

    function sanitizeUpdate(type, data) {
        data = data || {};
        if (type === 'playbackUpdate') {
            return {
                generationTime: toNumber(data.generationTime, Date.now()),
                time: Math.max(0, toNumber(data.time, 0)),
                duration: Math.max(0, toNumber(data.duration, 0)),
                state: toNumber(data.state, 0),
                speed: toNumber(data.speed, 1)
            };
        }
        if (type === 'volumeUpdate') {
            return {
                generationTime: toNumber(data.generationTime, Date.now()),
                volume: Math.max(0, Math.min(1, toNumber(data.volume, 1)))
            };
        }
        return { message: String(data.message || 'Playback error') };
    }

    function onUiMessage(entry, message) {
        if (entry !== ui) return;
        var type = message && message.type;
        var data = message && message.data;

        switch (type) {
            case 'hello':
                if (data && isModuleName(data.module)) moduleFullName = data.module;
                ui.visible = !data || data.visible !== false;
                sendToUi('deviceInfo', getDeviceInfo());
                sendToUi('connections', { count: sessions.length });
                deliverPendingPlay();
                break;
            case 'visibility':
                ui.visible = !!(data && data.visible);
                deliverPendingPlay();
                break;
            case 'getDeviceInfo':
                sendToUi('deviceInfo', getDeviceInfo());
                break;
            case 'playbackUpdate':
                broadcast(Opcode.PlaybackUpdate, sanitizeUpdate(type, data));
                break;
            case 'volumeUpdate':
                broadcast(Opcode.VolumeUpdate, sanitizeUpdate(type, data));
                break;
            case 'playbackError':
                broadcast(Opcode.PlaybackError, sanitizeUpdate(type, data));
                break;
            default:
                log('Unknown message from receiver page:', type);
        }
    }

    function onUiUpgrade(req, socket, head) {
        if (String(req.url || '').split('?')[0] !== '/ui') {
            socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
            return;
        }
        var ws = WebSocketConnection.accept(req, socket, head, 4 * 1024 * 1024);
        if (!ws) return;

        if (ui) {
            log('Replacing previous receiver page connection');
            var old = ui;
            ui = null;
            old.ws.onclose = null;
            old.ws.close(1000);
        }

        var entry = { ws: ws, visible: true };
        ui = entry;
        log('Receiver page connected');

        ws.onmessage = guard(function (data, binary) {
            if (binary) return;
            onUiMessage(entry, JSON.parse(data.toString('utf8')));
        });
        ws.onclose = function () {
            if (ui === entry) {
                ui = null;
                log('Receiver page disconnected');
            }
        };
    }

    // ---------------------------------------------------------------------
    // Media proxy and inline manifest store (loopback only)
    //
    // Receiver pages hosted by TizenBrew are subject to CORS, and players
    // cannot attach custom headers to media requests. XHR based players
    // (hls.js / dash.js) therefore load streams through
    //   http://127.0.0.1:46900/proxy/<scheme>/<host[:port]>/<path>
    // which keeps relative URLs inside manifests working.
    // ---------------------------------------------------------------------

    var PASSTHROUGH_RESPONSE_HEADERS = [
        'content-type', 'content-length', 'content-range', 'accept-ranges',
        'content-encoding', 'cache-control', 'expires', 'last-modified', 'etag'
    ];

    function corsHeaders(req, preflight) {
        var headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Content-Type, Accept-Ranges, Date'
        };
        if (preflight) {
            headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS';
            headers['Access-Control-Allow-Headers'] = req.headers['access-control-request-headers'] || '*';
            headers['Access-Control-Allow-Private-Network'] = 'true';
            headers['Access-Control-Max-Age'] = '600';
        }
        return headers;
    }

    function sendError(req, res, status, text) {
        if (res.headersSent) {
            try { res.end(); } catch (e) { /* ignore */ }
            return;
        }
        var headers = corsHeaders(req);
        headers['Content-Type'] = 'text/plain';
        res.writeHead(status, headers);
        res.end(text || '');
    }

    function proxyRequest(req, res, target, redirects) {
        var u = url.parse(target);
        var client = u.protocol === 'https:' ? https : u.protocol === 'http:' ? http : null;
        if (!client || !u.hostname) return sendError(req, res, 400, 'Unsupported URL');

        var headers = {};
        ['range', 'if-range', 'accept', 'user-agent'].forEach(function (name) {
            if (req.headers[name]) headers[name] = req.headers[name];
        });
        if (proxyHeaders) {
            Object.keys(proxyHeaders).forEach(function (name) {
                if (typeof proxyHeaders[name] === 'string') headers[name.toLowerCase()] = proxyHeaders[name];
            });
        }
        headers.host = u.host;

        var finished = false;
        var upstream = client.request({
            hostname: u.hostname,
            port: u.port || undefined,
            path: u.path || '/',
            method: req.method === 'HEAD' ? 'HEAD' : 'GET',
            headers: headers
        }, guard(function (upstreamRes) {
            var status = upstreamRes.statusCode;
            var location = upstreamRes.headers.location;
            if ((status === 301 || status === 302 || status === 303 || status === 307 || status === 308) && location) {
                upstreamRes.resume();
                if (redirects >= 5) return sendError(req, res, 508, 'Too many redirects');
                var next = url.resolve(target, location);
                if (/\.(m3u8|mpd)$/i.test(u.pathname || '') && /^https?:\/\//i.test(next)) {
                    // Let the player follow manifest redirects itself so that
                    // relative URLs resolve against the new location.
                    var redirectHeaders = corsHeaders(req);
                    redirectHeaders.Location = proxiedUrl(next);
                    res.writeHead(302, redirectHeaders);
                    res.end();
                    return;
                }
                return proxyRequest(req, res, next, redirects + 1);
            }

            var outHeaders = corsHeaders(req);
            PASSTHROUGH_RESPONSE_HEADERS.forEach(function (name) {
                if (upstreamRes.headers[name] !== undefined) outHeaders[name] = upstreamRes.headers[name];
            });
            res.writeHead(status, outHeaders);
            upstreamRes.on('error', function () {
                try { res.end(); } catch (e) { /* ignore */ }
            });
            upstreamRes.pipe(res);
        }, function () {
            sendError(req, res, 502, 'Proxy error');
        }));

        upstream.on('error', function (e) {
            if (!finished) log('Proxy request failed for ' + u.host + ':', e && e.message);
            sendError(req, res, 502, 'Upstream error: ' + (e && e.message));
        });
        upstream.setTimeout(30000, function () {
            upstream.abort();
        });
        res.on('finish', function () { finished = true; });
        res.on('close', function () {
            // The player aborted the request (seek, quality switch, stop).
            if (!finished) {
                finished = true;
                try { upstream.abort(); } catch (e) { /* ignore */ }
            }
        });
        upstream.end();
    }

    function proxiedUrl(target) {
        var match = /^(https?):\/\/(.*)$/i.exec(target);
        return UI_ORIGIN + '/proxy/' + match[1].toLowerCase() + '/' + match[2];
    }

    function handleProxy(req, res) {
        var match = /^\/proxy\/(https?)\/([^\/?#]+)(.*)$/i.exec(req.url);
        if (!match) return sendError(req, res, 400, 'Bad proxy URL');

        var origin = match[1].toLowerCase() + '://' + match[2];
        var rest = match[3] || '/';
        if (rest.charAt(0) !== '/') rest = '/' + rest;
        lastProxyOrigin = origin;
        proxyRequest(req, res, origin + rest, 0);
    }

    function handleContent(req, res) {
        var match = /^\/content\/(\d+)/.exec(req.url);
        var entry = match && contentStore[match[1]];
        if (!entry) return sendError(req, res, 404, 'Not found');

        var body = bufFrom(entry.body, 'utf8');
        var headers = corsHeaders(req);
        headers['Content-Type'] = entry.type;
        headers['Content-Length'] = String(body.length);
        headers['Cache-Control'] = 'no-cache';
        res.writeHead(200, headers);
        res.end(req.method === 'HEAD' ? undefined : body);
    }

    function handleUiRequest(req, res) {
        var path = req.url || '/';

        if (req.method === 'OPTIONS') {
            res.writeHead(204, corsHeaders(req, true));
            res.end();
            return;
        }
        if (req.method !== 'GET' && req.method !== 'HEAD') return sendError(req, res, 405, 'Method not allowed');

        if (path.indexOf('/proxy/') === 0) return handleProxy(req, res);
        if (path.indexOf('/content/') === 0) return handleContent(req, res);

        if (path === '/' || path === '/status') {
            var headers = corsHeaders(req);
            headers['Content-Type'] = 'application/json';
            res.writeHead(200, headers);
            res.end(JSON.stringify({
                service: 'TizenCast',
                version: VERSION,
                device: getDeviceInfo(),
                senders: sessions.length,
                receiverPage: !!ui
            }));
            return;
        }

        // Root-relative URLs inside proxied manifests ("/path/seg.ts")
        // resolve against the proxy origin; map them back upstream.
        if (lastProxyOrigin) return proxyRequest(req, res, lastProxyOrigin + path, 0);
        sendError(req, res, 404, 'Not found');
    }

    function startUiServer() {
        var server = http.createServer(function (req, res) {
            guard(handleUiRequest, function () {
                sendError(req, res, 500, 'Internal error');
            })(req, res);
        });
        server.on('clientError', function (e, socket) {
            try { socket.destroy(); } catch (x) { /* ignore */ }
        });
        server.on('connection', trackSocket);
        server.on('upgrade', guard(onUiUpgrade));
        listen(server, UI_PORT, UI_HOST, 'Receiver bridge');
    }

    // ---------------------------------------------------------------------
    // mDNS / DNS-SD advertisement
    // ---------------------------------------------------------------------

    var MDNS_ADDRESS = '224.0.0.251';
    var MDNS_PORT = 5353;
    var TYPE_A = 1;
    var TYPE_PTR = 12;
    var TYPE_TXT = 16;
    var TYPE_SRV = 33;
    var TYPE_ANY = 255;
    var SERVICES_META = '_services._dns-sd._udp.local';
    var SERVICE_TYPES = [
        { type: '_fcast._tcp.local', port: TCP_PORT },
        { type: '_fcast-ws._tcp.local', port: WS_PORT }
    ];

    var mdns = {
        socket: null,
        instance: 'TizenCast',
        host: 'TizenCast.local',
        memberships: [],
        addresses: '',
        lastSent: {}
    };

    function sanitizeLabel(text) {
        var label = String(text).replace(/[^\x20-\x7e]/g, '').replace(/\./g, '-').trim();
        return (label || 'TizenCast').substring(0, 63);
    }

    function encodeName(name) {
        var parts = [];
        name.split('.').forEach(function (label) {
            if (!label) return;
            var bytes = bufFrom(label, 'utf8');
            if (bytes.length > 63) bytes = bytes.slice(0, 63);
            parts.push(bufFrom([bytes.length]));
            parts.push(bytes);
        });
        parts.push(bufFrom([0]));
        return Buffer.concat(parts);
    }

    function readName(buf, offset) {
        var labels = [];
        var next = -1;
        var jumps = 0;
        while (true) {
            if (offset >= buf.length) throw new Error('Truncated name');
            var length = buf[offset];
            if (length === 0) {
                if (next < 0) next = offset + 1;
                break;
            }
            if ((length & 0xc0) === 0xc0) {
                if (offset + 1 >= buf.length || ++jumps > 32) throw new Error('Bad name pointer');
                if (next < 0) next = offset + 2;
                offset = ((length & 0x3f) << 8) | buf[offset + 1];
                continue;
            }
            if (length & 0xc0) throw new Error('Bad label');
            labels.push(buf.toString('utf8', offset + 1, offset + 1 + length));
            offset += 1 + length;
        }
        return { name: labels.join('.'), next: next };
    }

    function parseQuery(buf) {
        if (buf.length < 12) return null;
        var flags = buf.readUInt16BE(2);
        if (flags & 0x8000) return null; // a response, not a query
        var count = buf.readUInt16BE(4);
        var questions = [];
        var offset = 12;
        for (var i = 0; i < count; i++) {
            var name = readName(buf, offset);
            offset = name.next;
            if (offset + 4 > buf.length) break;
            questions.push({ name: name.name, type: buf.readUInt16BE(offset), cls: buf.readUInt16BE(offset + 2) });
            offset += 4;
        }
        return { id: buf.readUInt16BE(0), questions: questions };
    }

    function instanceName(service) {
        return mdns.instance + '.' + service.type;
    }

    function record(name, type, ttl, data, unique) {
        return { name: name, type: type, ttl: ttl, data: data, unique: !!unique };
    }

    function ptrRecord(name, target) {
        return record(name, TYPE_PTR, 4500, encodeName(target), false);
    }

    function srvRecord(service) {
        var head = bufAlloc(6);
        head.writeUInt16BE(0, 0);             // priority
        head.writeUInt16BE(0, 2);             // weight
        head.writeUInt16BE(service.port, 4);
        return record(instanceName(service), TYPE_SRV, 120, Buffer.concat([head, encodeName(mdns.host)]), true);
    }

    function txtRecord(service) {
        return record(instanceName(service), TYPE_TXT, 4500, bufFrom([0]), true);
    }

    function addressRecords() {
        return getIPv4Addresses().map(function (ip) {
            return record(mdns.host, TYPE_A, 120, bufFrom(ip.split('.').map(Number)), true);
        });
    }

    function encodeRecord(r, legacy) {
        var header = bufAlloc(10);
        header.writeUInt16BE(r.type, 0);
        header.writeUInt16BE((r.unique && !legacy ? 0x8000 : 0) | 1, 2);
        header.writeUInt32BE(legacy ? Math.min(r.ttl, 10) : r.ttl, 4);
        header.writeUInt16BE(r.data.length, 8);
        return Buffer.concat([encodeName(r.name), header, r.data]);
    }

    function encodeResponse(id, questions, answers, additionals, legacy) {
        var header = bufAlloc(12);
        header.writeUInt16BE(id, 0);
        header.writeUInt16BE(0x8400, 2); // response, authoritative
        header.writeUInt16BE(questions.length, 4);
        header.writeUInt16BE(answers.length, 6);
        header.writeUInt16BE(0, 8);
        header.writeUInt16BE(additionals.length, 10);
        var parts = [header];
        questions.forEach(function (q) {
            parts.push(encodeName(q.name), uint16(q.type), uint16(q.cls & 0x7fff));
        });
        answers.forEach(function (r) { parts.push(encodeRecord(r, legacy)); });
        additionals.forEach(function (r) { parts.push(encodeRecord(r, legacy)); });
        return Buffer.concat(parts);
    }

    function answerQuestion(question, answers, additionals) {
        var name = question.name.toLowerCase();
        var type = question.type;
        var any = type === TYPE_ANY;

        if (name === SERVICES_META && (type === TYPE_PTR || any)) {
            SERVICE_TYPES.forEach(function (s) { answers.push(ptrRecord(SERVICES_META, s.type)); });
        }

        SERVICE_TYPES.forEach(function (s) {
            if (name === s.type && (type === TYPE_PTR || any)) {
                answers.push(ptrRecord(s.type, instanceName(s)));
                additionals.push(srvRecord(s), txtRecord(s));
                Array.prototype.push.apply(additionals, addressRecords());
            }
            if (name === instanceName(s).toLowerCase()) {
                if (type === TYPE_SRV || any) answers.push(srvRecord(s));
                if (type === TYPE_TXT || any) answers.push(txtRecord(s));
                if (type === TYPE_SRV || any) Array.prototype.push.apply(additionals, addressRecords());
            }
        });

        if (name === mdns.host.toLowerCase() && (type === TYPE_A || any)) {
            Array.prototype.push.apply(answers, addressRecords());
        }
    }

    function dedupe(records, exclude) {
        var seen = {};
        (exclude || []).forEach(function (r) { seen[r.name.toLowerCase() + '|' + r.type + '|' + r.data.toString('hex')] = true; });
        return records.filter(function (r) {
            var key = r.name.toLowerCase() + '|' + r.type + '|' + r.data.toString('hex');
            if (seen[key]) return false;
            seen[key] = true;
            return true;
        });
    }

    function mdnsSend(packet, port, address) {
        if (!mdns.socket) return;
        mdns.socket.send(packet, 0, packet.length, port, address, function (e) {
            if (e) log('mDNS send failed:', e.message);
        });
    }

    function handleMdnsMessage(msg, rinfo) {
        var query = parseQuery(msg);
        if (!query || query.questions.length === 0) return;

        var answers = [];
        var additionals = [];
        var unicastRequested = false;
        query.questions.forEach(function (q) {
            if (q.cls & 0x8000) unicastRequested = true;
            answerQuestion(q, answers, additionals);
        });
        answers = dedupe(answers);
        if (answers.length === 0) return;
        additionals = dedupe(additionals, answers);

        if (rinfo.port !== MDNS_PORT) {
            // Legacy unicast query (RFC 6762 section 6.7).
            mdnsSend(encodeResponse(query.id, query.questions, answers, additionals, true), rinfo.port, rinfo.address);
            return;
        }

        var packet = encodeResponse(0, [], answers, additionals, false);
        if (unicastRequested) mdnsSend(packet, rinfo.port, rinfo.address);

        // Do not multicast identical answers more than once per second.
        var key = packet.toString('base64');
        var now = Date.now();
        if (mdns.lastSent[key] && now - mdns.lastSent[key] < 1000) return;
        mdns.lastSent = {};
        mdns.lastSent[key] = now;
        mdnsSend(packet, MDNS_PORT, MDNS_ADDRESS);
    }

    function allRecords() {
        var answers = [];
        SERVICE_TYPES.forEach(function (s) {
            answers.push(ptrRecord(SERVICES_META, s.type), ptrRecord(s.type, instanceName(s)), srvRecord(s), txtRecord(s));
        });
        return answers.concat(addressRecords());
    }

    function announce(times, delay) {
        if (!mdns.socket || stopped) return;
        mdnsSend(encodeResponse(0, [], allRecords(), [], false), MDNS_PORT, MDNS_ADDRESS);
        if (times > 1) later(function () { announce(times - 1, delay * 2); }, delay);
    }

    function mdnsGoodbye() {
        if (!mdns.socket) return;
        var records = allRecords().map(function (r) {
            return record(r.name, r.type, 0, r.data, r.unique);
        });
        mdnsSend(encodeResponse(0, [], records, [], false), MDNS_PORT, MDNS_ADDRESS);
    }

    function joinMulticastGroups() {
        var socket = mdns.socket;
        if (!socket) return;
        var addresses = getIPv4Addresses();
        addresses.forEach(function (address) {
            if (mdns.memberships.indexOf(address) >= 0) return;
            try {
                socket.addMembership(MDNS_ADDRESS, address);
                mdns.memberships.push(address);
            } catch (e) {
                log('mDNS: could not join group on ' + address + ':', e && e.message);
            }
        });
        if (mdns.memberships.length === 0 && mdns.memberships.indexOf('default') < 0) {
            try {
                socket.addMembership(MDNS_ADDRESS);
                mdns.memberships.push('default');
            } catch (e) {
                log('mDNS: could not join multicast group:', e && e.message);
            }
        }
    }

    function startMdns() {
        var suffix = crypto.createHash('sha1').update(String(device.id || crypto.randomBytes(8).toString('hex'))).digest('hex').substring(0, 6);
        mdns.instance = sanitizeLabel(device.name);
        mdns.host = 'TizenCast-' + suffix + '.local';

        var socket;
        try {
            socket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        } catch (e) {
            log('mDNS unavailable:', errorText(e));
            return;
        }

        socket.on('error', function (e) {
            log('mDNS error (automatic discovery disabled):', e && e.message);
            try { socket.close(); } catch (x) { /* ignore */ }
            if (mdns.socket === socket) mdns.socket = null;
            sendToUi('deviceInfo', getDeviceInfo());
        });
        socket.on('message', guard(handleMdnsMessage));
        socket.on('listening', guard(function () {
            mdns.socket = socket;
            try { socket.setMulticastTTL(255); } catch (e) { /* ignore */ }
            try { socket.setMulticastLoopback(true); } catch (e) { /* ignore */ }
            joinMulticastGroups();
            mdns.addresses = getIPv4Addresses().join(',');
            log('mDNS advertising "' + mdns.instance + '" as ' + mdns.host);
            announce(3, 1000);
            sendToUi('deviceInfo', getDeviceInfo());
        }));
        socket.bind(MDNS_PORT);

        // Re-announce when the TV's addresses change (Wi-Fi reconnects etc.).
        every(function () {
            var addresses = getIPv4Addresses().join(',');
            if (addresses === mdns.addresses) return;
            mdns.addresses = addresses;
            joinMulticastGroups();
            announce(2, 1000);
            sendToUi('deviceInfo', getDeviceInfo());
        }, 15000);
    }

    // ---------------------------------------------------------------------
    // Start
    // ---------------------------------------------------------------------

    log('Starting TizenCast service ' + VERSION + ' on Node ' + process.version);
    try {
        startUiServer();
        startTcpListener();
        startWebSocketListener();
    } catch (e) {
        log('Failed to start listeners:', errorText(e));
    }
    loadDeviceInfo(guard(function () {
        log('Receiver name: ' + device.name);
        sendToUi('deviceInfo', getDeviceInfo());
        if (!stopped) startMdns();
    }));
})();
