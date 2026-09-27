/*
 * TizenCast receiver page.
 *
 * TizenBrew serves this page from http://127.0.0.1:8081/module/<module>/app/cast.html.
 * Pages loaded that way have no access to the `tizen`/`webapis` objects, so all
 * device and network work happens in the TizenCast service (service.js), which
 * this page talks to over a WebSocket on 127.0.0.1:46900.
 *
 * Plain ES5 on purpose: Tizen 3/4 TVs run old Chromium builds.
 */
(function () {
    'use strict';

    var VERSION = '0.7.0';
    var SERVICE_ORIGIN = 'http://127.0.0.1:46900';
    var BRIDGE_URL = 'ws://127.0.0.1:46900/ui';
    var LIBRARIES = {
        hls: 'https://cdn.jsdelivr.net/npm/hls.js@1.5.20/dist/hls.min.js',
        dash: 'https://cdn.jsdelivr.net/npm/dashjs@4.7.4/dist/dash.all.min.js',
        qrcode: 'https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.js'
    };
    var HLS_TYPES = ['application/vnd.apple.mpegurl', 'application/x-mpegurl', 'audio/mpegurl', 'audio/x-mpegurl'];
    // dash.js error codes that indicate a failed download (see dashjs Errors).
    var DASH_NETWORK_ERRORS = [11, 12, 15, 17, 25, 26, 27, 28, 29];

    var Key = {
        ENTER: 13,
        LEFT: 37,
        UP: 38,
        RIGHT: 39,
        DOWN: 40,
        BACK: 10009,
        PLAY: 415,
        PAUSE: 19,
        PLAY_PAUSE: 10252,
        STOP: 413,
        REWIND: 412,
        FAST_FORWARD: 417
    };
    var SEEK_STEP = 10;
    var OSD_TIMEOUT = 4000;
    var IDLE_AFTER_END = 10000;
    var SERVICE_START_GRACE = 15000;

    function $(id) {
        return document.getElementById(id);
    }

    var video = $('video');
    var els = {
        status: $('status'),
        statusText: $('status-text'),
        statusHint: $('status-hint'),
        deviceName: $('device-name'),
        addresses: $('addresses'),
        ports: $('ports'),
        discovery: $('discovery'),
        qrBox: $('qr-box'),
        qrCode: $('qr-code'),
        osd: $('osd'),
        osdState: $('osd-state'),
        osdTime: $('osd-time'),
        osdBuffer: $('osd-buffer'),
        osdProgress: $('osd-progress'),
        toasts: $('toasts')
    };

    // ---------------------------------------------------------------------
    // Utilities
    // ---------------------------------------------------------------------

    var scripts = {};

    function loadScript(src, callback) {
        var entry = scripts[src];
        if (entry && entry.state === 'loaded') {
            callback(null);
            return;
        }
        if (entry && entry.state === 'loading') {
            entry.callbacks.push(callback);
            return;
        }

        entry = scripts[src] = { state: 'loading', callbacks: [callback] };
        var el = document.createElement('script');

        function finish(error) {
            entry.state = error ? 'failed' : 'loaded';
            var callbacks = entry.callbacks;
            entry.callbacks = [];
            if (error && el.parentNode) el.parentNode.removeChild(el);
            callbacks.forEach(function (cb) { cb(error); });
        }

        el.src = src;
        el.async = true;
        el.onload = function () { finish(null); };
        el.onerror = function () { finish(new Error('Failed to load ' + src)); };
        document.getElementsByTagName('head')[0].appendChild(el);
    }

    function finite(value) {
        return typeof value === 'number' && isFinite(value) ? value : 0;
    }

    function pad(n) {
        return n < 10 ? '0' + n : String(n);
    }

    function formatTime(seconds) {
        seconds = Math.floor(finite(seconds));
        var h = Math.floor(seconds / 3600);
        var m = Math.floor((seconds % 3600) / 60);
        var s = seconds % 60;
        return (h > 0 ? h + ':' + pad(m) : pad(m)) + ':' + pad(s);
    }

    function base64Url(text) {
        return window.btoa(unescape(encodeURIComponent(text)))
            .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    }

    // Routes a request through the TizenCast service, which adds CORS headers
    // and the sender's custom headers. The path keeps the original URL
    // structure so relative URLs inside manifests keep resolving.
    function proxify(url) {
        if (!url || url.indexOf(SERVICE_ORIGIN + '/') === 0) return url;
        var match = /^(https?):\/\/([^#]*)/i.exec(url);
        return match ? SERVICE_ORIGIN + '/proxy/' + match[1].toLowerCase() + '/' + match[2] : url;
    }

    function moduleName() {
        var match = /\/module\/([^\/]+)\//.exec(location.pathname);
        if (!match) return null;
        try {
            return decodeURIComponent(match[1]);
        } catch (e) {
            return null;
        }
    }

    function toast(message, isError) {
        var el = document.createElement('div');
        el.className = 'toast' + (isError ? ' error' : '');
        el.textContent = message;
        els.toasts.appendChild(el);
        while (els.toasts.children.length > 3) els.toasts.removeChild(els.toasts.firstChild);

        setTimeout(function () {
            el.className += ' hide';
            setTimeout(function () {
                if (el.parentNode) el.parentNode.removeChild(el);
            }, 600);
        }, isError ? 8000 : 4000);
    }

    // ---------------------------------------------------------------------
    // Service bridge
    // ---------------------------------------------------------------------

    var bridge = { socket: null, connected: false, startedAt: Date.now() };
    var senderCount = 0;

    function connectBridge() {
        var socket;
        try {
            socket = new WebSocket(BRIDGE_URL);
        } catch (e) {
            setTimeout(connectBridge, 1000);
            return;
        }
        bridge.socket = socket;

        socket.onopen = function () {
            bridge.connected = true;
            send('hello', { module: moduleName(), visible: !document.hidden, version: VERSION });
            updateStatus();
        };
        socket.onmessage = function (event) {
            var message;
            try {
                message = JSON.parse(event.data);
            } catch (e) {
                return;
            }
            handleServiceMessage(message.type, message.data || {});
        };
        socket.onclose = function () {
            if (bridge.socket !== socket) return;
            bridge.socket = null;
            bridge.connected = false;
            bridge.startedAt = Date.now();
            updateStatus();
            setTimeout(connectBridge, 1000);
        };
    }

    function send(type, data) {
        if (!bridge.connected || !bridge.socket) return;
        try {
            bridge.socket.send(JSON.stringify({ type: type, data: data }));
        } catch (e) { /* reconnect handles it */ }
    }

    function handleServiceMessage(type, data) {
        switch (type) {
            case 'deviceInfo':
                renderDeviceInfo(data);
                break;
            case 'connections':
                senderCount = data.count || 0;
                updateStatus();
                break;
            case 'connect':
                senderCount = data.count || 0;
                updateStatus();
                toast('Device connected');
                break;
            case 'disconnect':
                senderCount = data.count || 0;
                updateStatus();
                if (senderCount === 0) toast('Device disconnected');
                break;
            case 'play':
                startPlayback(data);
                break;
            case 'pause':
                if (session) video.pause();
                break;
            case 'resume':
                if (session) playVideo();
                break;
            case 'stop':
                stopPlayback();
                showIdle();
                break;
            case 'seek':
                seekTo(Number(data.time));
                break;
            case 'setvolume':
                setVolume(Number(data.volume));
                break;
            case 'setspeed':
                setSpeed(Number(data.speed));
                break;
            case 'toast':
                toast(data.message, data.error);
                break;
        }
    }

    // ---------------------------------------------------------------------
    // Idle screen
    // ---------------------------------------------------------------------

    var lastQrText = null;

    function setStatus(text, state, hint) {
        els.statusText.textContent = text;
        els.status.className = state || '';
        els.statusHint.textContent = hint || '';
    }

    function updateStatus() {
        if (!bridge.connected) {
            if (Date.now() - bridge.startedAt > SERVICE_START_GRACE) {
                setStatus('TizenCast service is not running', 'error',
                    'Go back to TizenBrew and open TizenCast again.');
            } else {
                setStatus('Starting TizenCast service\u2026', '');
            }
        } else if (senderCount > 0) {
            setStatus('Connected \u2013 ready to cast', 'connected');
        } else {
            setStatus('Waiting for a connection', '', 'Cast from an FCast sender app such as Grayjay.');
        }
    }

    function renderDeviceInfo(info) {
        var addresses = info.addresses || [];
        var ports = info.ports || { tcp: 46899, ws: 46898 };

        els.deviceName.textContent = info.name || '';
        els.addresses.innerHTML = '';
        if (addresses.length === 0) {
            var none = document.createElement('div');
            none.textContent = 'Not connected to a network';
            els.addresses.appendChild(none);
        }
        addresses.forEach(function (address) {
            var row = document.createElement('div');
            row.textContent = address;
            els.addresses.appendChild(row);
        });
        els.ports.textContent = ports.tcp + ' (TCP), ' + ports.ws + ' (WebSocket)';
        els.discovery.textContent = info.discovery
            ? 'Automatic discovery is available via mDNS.'
            : 'Automatic discovery is unavailable; connect with the IP address or QR code.';

        renderQrCode(info.name || 'TizenCast', addresses, ports);
    }

    function renderQrCode(name, addresses, ports) {
        if (addresses.length === 0) {
            els.qrBox.style.display = 'none';
            lastQrText = null;
            return;
        }

        var text = 'fcast://r/' + base64Url(JSON.stringify({
            name: name,
            addresses: addresses,
            services: [{ port: ports.tcp, type: 0 }, { port: ports.ws, type: 1 }]
        }));
        if (text === lastQrText) return;

        loadScript(LIBRARIES.qrcode, function (error) {
            if (error || typeof window.qrcode !== 'function') {
                els.qrBox.style.display = 'none';
                return;
            }
            var qr = window.qrcode(0, 'M');
            qr.addData(text);
            qr.make();
            els.qrCode.innerHTML = qr.createSvgTag({ cellSize: 4, margin: 0, scalable: true });
            els.qrBox.style.display = '';
            lastQrText = text;
        });
    }

    function showIdle() {
        document.body.className = '';
        hideOsd();
    }

    function showPlayer() {
        if (document.body.className.indexOf('casting') < 0) document.body.className = 'casting';
    }

    // ---------------------------------------------------------------------
    // Playback
    // ---------------------------------------------------------------------

    var session = null;
    var sessionCounter = 0;
    var volume = 1;
    var lastGenerationTime = 0;
    var idleTimer = null;
    var osdTimer = null;

    function detectMode(play) {
        var container = String(play.container || '').toLowerCase();
        var path = String(play.url || '').toLowerCase().split('?')[0];
        if (container === 'application/dash+xml' || (!container && /\.mpd$/.test(path))) return 'dash';
        if (HLS_TYPES.indexOf(container) >= 0 || (!container && /\.m3u8$/.test(path))) return 'hls';
        return 'html';
    }

    function isSameMedia(a, b) {
        return (a.url || null) === (b.url || null) && (a.content || null) === (b.content || null);
    }

    function startPlayback(play) {
        if (!play || (!play.url && !play.contentUrl)) return;

        if (session && isSameMedia(session.play, play)) {
            // Senders re-send Play for media that is already loaded.
            if (typeof play.time === 'number' && Math.abs(play.time - video.currentTime) > 5) seekTo(play.time);
            if (typeof play.speed === 'number') setSpeed(play.speed);
            if (video.paused && !video.ended) playVideo();
            showPlayer();
            return;
        }

        stopPlayback();

        var hasHeaders = !!play.headers && Object.keys(play.headers).length > 0;
        var s = {
            id: ++sessionCounter,
            play: play,
            mode: detectMode(play),
            source: play.contentUrl || play.url,
            useProxy: false,
            triedFallback: false,
            recoveredMedia: false,
            resumeAt: -1,
            startApplied: false,
            volumeSent: false,
            lastUpdateAt: -1,
            live: false
        };

        // Prefer the TV's native HLS support unless the stream needs headers.
        if (s.mode === 'hls' && !hasHeaders && !play.contentUrl &&
            video.canPlayType(play.container || 'application/vnd.apple.mpegurl')) {
            s.mode = 'html';
        }
        // hls.js and dash.js fetch with XHR, which is subject to CORS on pages
        // hosted by TizenBrew, so they go through the service's proxy.
        s.useProxy = s.mode !== 'html' || hasHeaders;

        session = s;
        clearTimeout(idleTimer);
        showPlayer();
        setBuffering(true);
        showOsd();
        loadSession(s);
    }

    function loadSession(s) {
        if (s.mode === 'dash') loadDash(s);
        else if (s.mode === 'hls') loadHls(s);
        else loadHtml(s);
    }

    function startTime(s) {
        if (s.resumeAt > 0) return s.resumeAt;
        return typeof s.play.time === 'number' && s.play.time > 0 ? s.play.time : 0;
    }

    function loadHtml(s) {
        video.src = s.useProxy ? proxify(s.source) : s.source;
        video.load();
        playVideo();
    }

    function loadHls(s) {
        loadScript(LIBRARIES.hls, function (error) {
            if (session !== s) return;
            var Hls = window.Hls;
            if (error || !Hls || !Hls.isSupported()) {
                if (!s.play.contentUrl && video.canPlayType('application/vnd.apple.mpegurl')) {
                    s.mode = 'html';
                    loadHtml(s);
                } else {
                    fail(s, 'HLS playback is not supported on this TV');
                }
                return;
            }

            var hls = new Hls({
                startPosition: startTime(s) > 0 ? startTime(s) : -1,
                xhrSetup: function (xhr, url) {
                    if (s.useProxy) xhr.open('GET', proxify(url), true);
                }
            });
            s.hls = hls;

            hls.on(Hls.Events.LEVEL_LOADED, function (event, data) {
                s.live = !!(data && data.details && data.details.live);
            });
            hls.on(Hls.Events.ERROR, function (event, data) {
                if (session !== s || !data || !data.fatal) return;
                if (data.type === Hls.ErrorTypes.NETWORK_ERROR && retryOtherRoute(s)) return;
                if (data.type === Hls.ErrorTypes.MEDIA_ERROR && !s.recoveredMedia) {
                    s.recoveredMedia = true;
                    hls.recoverMediaError();
                    return;
                }
                fail(s, 'HLS playback failed (' + data.details + ')');
            });

            hls.loadSource(s.source);
            hls.attachMedia(video);
            playVideo();
        });
    }

    function loadDash(s) {
        loadScript(LIBRARIES.dash, function (error) {
            if (session !== s) return;
            var dashjs = window.dashjs;
            if (error || !dashjs) {
                fail(s, 'Could not load the DASH player');
                return;
            }

            var player = dashjs.MediaPlayer().create();
            s.dash = player;
            player.extend('RequestModifier', function () {
                return {
                    modifyRequestURL: function (url) {
                        return s.useProxy ? proxify(url) : url;
                    },
                    modifyRequestHeader: function (xhr) {
                        return xhr;
                    }
                };
            }, true);

            player.on(dashjs.MediaPlayer.events.ERROR, function (event) {
                if (session !== s) return;
                var err = event && event.error || {};
                if (DASH_NETWORK_ERRORS.indexOf(err.code) >= 0 && retryOtherRoute(s)) return;
                fail(s, 'DASH playback failed (' + (err.message || err.code || 'unknown error') + ')');
            });
            player.on(dashjs.MediaPlayer.events.PLAYBACK_ERROR, function (event) {
                if (session !== s) return;
                var err = event && event.error;
                fail(s, 'DASH playback failed (' + (err && (err.message || err.code) || 'media error') + ')');
            });

            var start = startTime(s);
            player.initialize(video, s.source, true, start > 0 ? start : NaN);
        });
    }

    // Switches between direct and proxied loading once, e.g. when the proxy
    // cannot reach a host or a server rejects direct requests.
    function retryOtherRoute(s) {
        if (s.triedFallback) return false;
        s.triedFallback = true;
        s.useProxy = !s.useProxy;
        s.resumeAt = video.currentTime > 0 ? video.currentTime : -1;
        s.startApplied = false;
        console.warn('TizenCast: retrying ' + (s.useProxy ? 'through the proxy' : 'directly'));
        teardown(s);
        setBuffering(true);
        loadSession(s);
        return true;
    }

    function teardown(s) {
        if (s.hls) {
            try { s.hls.destroy(); } catch (e) { /* ignore */ }
            s.hls = null;
        }
        if (s.dash) {
            try { s.dash.destroy(); } catch (e) { /* ignore */ }
            s.dash = null;
        }
        try { video.pause(); } catch (e) { /* ignore */ }
        video.removeAttribute('src');
        try { video.load(); } catch (e) { /* ignore */ }
    }

    function stopPlayback() {
        var s = session;
        if (!s) return;
        session = null;
        clearTimeout(idleTimer);
        teardown(s);
        setBuffering(false);
    }

    // Playback stopped on the TV (Back/Stop key); let the sender know.
    function stopFromRemote() {
        if (!session) return;
        stopPlayback();
        send('playbackUpdate', { generationTime: nextGenerationTime(), time: 0, duration: 0, state: 0, speed: 1 });
        showIdle();
    }

    function fail(s, message) {
        if (session !== s) return;
        console.error('TizenCast: ' + message);
        send('playbackError', { message: message });
        toast(message, true);
        stopPlayback();
        send('playbackUpdate', { generationTime: nextGenerationTime(), time: 0, duration: 0, state: 0, speed: 1 });
        showIdle();
    }

    function playVideo() {
        try {
            var result = video.play();
            if (result && typeof result.catch === 'function') result.catch(function () { /* handled by events */ });
        } catch (e) { /* ignore */ }
    }

    function togglePause() {
        if (!session) return;
        if (video.paused) playVideo();
        else video.pause();
        showOsd();
    }

    function seekTo(time) {
        if (!session || !isFinite(time)) return;
        var target = Math.max(0, time);
        var ranges = video.seekable;
        if (ranges && ranges.length) {
            target = Math.min(Math.max(target, ranges.start(0)), ranges.end(ranges.length - 1));
        }
        try {
            video.currentTime = target;
        } catch (e) { /* not seekable yet */ }
        showOsd();
    }

    function setVolume(value) {
        if (!isFinite(value)) return;
        volume = Math.max(0, Math.min(1, value));
        video.muted = false;
        video.volume = volume;
    }

    function setSpeed(value) {
        if (!isFinite(value) || value <= 0) return;
        video.playbackRate = Math.max(0.0625, Math.min(16, value));
    }

    function nextGenerationTime() {
        var now = Date.now();
        if (now <= lastGenerationTime) now = lastGenerationTime + 1;
        lastGenerationTime = now;
        return now;
    }

    function sendPlaybackUpdate(state) {
        if (!session) return;
        send('playbackUpdate', {
            generationTime: nextGenerationTime(),
            time: finite(video.currentTime),
            duration: finite(video.duration),
            state: state,
            speed: video.playbackRate || 1
        });
    }

    function sendVolumeUpdate() {
        send('volumeUpdate', { generationTime: nextGenerationTime(), volume: video.muted ? 0 : video.volume });
    }

    function currentState() {
        if (video.ended) return 0;
        return video.paused ? 2 : 1;
    }

    // ---------------------------------------------------------------------
    // On-screen display
    // ---------------------------------------------------------------------

    function setBuffering(on) {
        var cls = document.body.className.replace(/\s*buffering/g, '');
        document.body.className = on ? cls + ' buffering' : cls;
    }

    function updateOsd() {
        if (!session) return;
        var duration = video.duration;
        var time = video.currentTime;
        var live = session.live || duration === Infinity;

        els.osdState.className = video.paused ? 'paused' : 'playing';
        els.osd.className = els.osd.className.replace(/\s*live/g, '') + (live ? ' live' : '');

        if (live || !(duration > 0)) {
            els.osdTime.textContent = formatTime(time);
            els.osdProgress.style.width = live ? '100%' : '0';
            els.osdBuffer.style.width = '0';
            return;
        }

        var buffered = 0;
        for (var i = 0; i < video.buffered.length; i++) {
            if (video.buffered.start(i) <= time && time <= video.buffered.end(i)) buffered = video.buffered.end(i);
        }
        els.osdTime.textContent = formatTime(time) + ' / ' + formatTime(duration);
        els.osdProgress.style.width = Math.min(100, time / duration * 100) + '%';
        els.osdBuffer.style.width = Math.min(100, buffered / duration * 100) + '%';
    }

    function showOsd() {
        updateOsd();
        if (els.osd.className.indexOf('visible') < 0) els.osd.className += ' visible';
        clearTimeout(osdTimer);
        if (!video.paused) osdTimer = setTimeout(hideOsd, OSD_TIMEOUT);
    }

    function hideOsd() {
        clearTimeout(osdTimer);
        els.osd.className = els.osd.className.replace(/\s*visible/g, '');
    }

    // ---------------------------------------------------------------------
    // Video element events
    // ---------------------------------------------------------------------

    video.addEventListener('loadedmetadata', function () {
        var s = session;
        if (!s) return;
        if (s.mode === 'html' && !s.startApplied) {
            s.startApplied = true;
            var start = startTime(s);
            if (start > 0) seekTo(start);
        }
        if (typeof s.play.speed === 'number') setSpeed(s.play.speed);
        video.volume = volume;
        if (!s.volumeSent) {
            // The Play message carries no volume, so tell the sender ours.
            s.volumeSent = true;
            sendVolumeUpdate();
        }
        updateOsd();
    });

    video.addEventListener('playing', function () {
        if (!session) return;
        setBuffering(false);
        sendPlaybackUpdate(1);
        showOsd();
    });

    video.addEventListener('pause', function () {
        if (!session || video.ended) return;
        sendPlaybackUpdate(2);
        showOsd();
    });

    video.addEventListener('ended', function () {
        var s = session;
        if (!s) return;
        sendPlaybackUpdate(0);
        showOsd();
        clearTimeout(idleTimer);
        idleTimer = setTimeout(function () {
            if (session === s && video.ended) {
                stopPlayback();
                showIdle();
            }
        }, IDLE_AFTER_END);
    });

    video.addEventListener('timeupdate', function () {
        var s = session;
        if (!s) return;
        updateOsd();
        if (Math.abs(video.currentTime - s.lastUpdateAt) >= 1) {
            s.lastUpdateAt = video.currentTime;
            sendPlaybackUpdate(currentState());
        }
    });

    video.addEventListener('seeked', function () {
        if (!session) return;
        sendPlaybackUpdate(currentState());
        updateOsd();
    });

    video.addEventListener('ratechange', function () {
        if (session) sendPlaybackUpdate(currentState());
    });

    video.addEventListener('volumechange', function () {
        if (session) sendVolumeUpdate();
    });

    video.addEventListener('waiting', function () {
        if (session) setBuffering(true);
    });

    video.addEventListener('canplay', function () {
        if (session) setBuffering(false);
    });

    video.addEventListener('progress', function () {
        if (session) updateOsd();
    });

    video.addEventListener('error', function () {
        var s = session;
        // hls.js and dash.js report their own errors.
        if (!s || s.mode !== 'html' || !video.error) return;
        var code = video.error.code;
        if ((code === 2 || code === 4) && retryOtherRoute(s)) return;
        var reasons = { 1: 'aborted', 2: 'network error', 3: 'decoding error', 4: 'unsupported format or unreachable source' };
        fail(s, 'Playback failed (' + (reasons[code] || 'error ' + code) + ')');
    });

    // ---------------------------------------------------------------------
    // Remote control
    // ---------------------------------------------------------------------

    document.addEventListener('keydown', function (event) {
        var handled = true;

        if (session) {
            switch (event.keyCode) {
                case Key.ENTER:
                case Key.PLAY_PAUSE:
                    togglePause();
                    break;
                case Key.PLAY:
                    playVideo();
                    showOsd();
                    break;
                case Key.PAUSE:
                    video.pause();
                    showOsd();
                    break;
                case Key.LEFT:
                case Key.REWIND:
                    seekTo(video.currentTime - SEEK_STEP);
                    break;
                case Key.RIGHT:
                case Key.FAST_FORWARD:
                    seekTo(video.currentTime + SEEK_STEP);
                    break;
                case Key.UP:
                case Key.DOWN:
                    showOsd();
                    break;
                case Key.BACK:
                case Key.STOP:
                    stopFromRemote();
                    break;
                default:
                    handled = false;
            }
        } else if (event.keyCode === Key.BACK) {
            // Return to TizenBrew; the service keeps listening and reopens
            // this page when something is cast.
            if (window.history.length > 1) window.history.back();
        } else {
            handled = false;
        }

        if (handled) event.preventDefault();
    });

    document.addEventListener('visibilitychange', function () {
        send('visibility', { visible: !document.hidden });
    });

    // ---------------------------------------------------------------------
    // Start
    // ---------------------------------------------------------------------

    updateStatus();
    setInterval(updateStatus, 3000);
    connectBridge();
})();
