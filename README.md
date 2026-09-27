# TizenCast

An [FCast](https://fcast.org) receiver for Samsung Tizen TVs, packaged as a
[TizenBrew](https://github.com/reisxd/TizenBrew) module. Cast videos to your TV
from [Grayjay](https://grayjay.app) or any other FCast sender app.

It follows the official
[FCast Tizen receiver](https://github.com/futo-org/fcast/releases/tag/tizen-v1.0.0)
(protocol version 2), reworked to run inside TizenBrew instead of as a
separately installed `.wgt` + .NET service.

## Install

1. Install TizenBrew on your TV.
2. In TizenBrew, open the module manager and choose **Add GitHub Module**.
3. Enter `BlindeCode/TizenCast`.
4. Launch **TizenCast** from the module list.

The TV then shows up in your sender app's cast list (via mDNS). If it doesn't,
connect manually with the IP address shown on screen, or scan the QR code with
the sender app.

To receive casts without opening TizenCast first, add it under
**Settings → Autolaunch Settings for services** in TizenBrew. The service
then starts together with TizenBrew and opens the receiver when something is
cast.

## Remote control

| Key | Action |
| --- | --- |
| OK / Play-Pause | Play or pause |
| Left / Rewind | Back 10 seconds |
| Right / Fast-forward | Forward 10 seconds |
| Up / Down | Show progress |
| Back / Stop | Stop playback (on the idle screen, Back returns to TizenBrew) |

## How it works

TizenBrew serves `app/cast.html` to the TV browser and runs `app/service.js`
in its Node.js service.

- **`app/service.js`** (Node.js, ES5, built-in modules only)
  - FCast over TCP on port 46899 and over WebSocket on port 46898.
  - Advertises `_fcast._tcp` and `_fcast-ws._tcp` over mDNS.
  - Talks to the receiver page over a WebSocket on `127.0.0.1:46900/ui`.
  - Proxies media for hls.js/dash.js on `127.0.0.1:46900/proxy/...`. This
    adds CORS headers and any custom headers the sender asked for.
  - When a cast arrives and the receiver page isn't open, it opens the page
    through TizenBrew's app control.
- **`app/main.js`** (browser, ES5) plays media with the TV's `<video>`
  element. It uses [hls.js](https://github.com/video-dev/hls.js) and
  [dash.js](https://github.com/Dash-Industry-Forum/dash.js), loaded from
  jsDelivr, when a stream needs them. It also sends playback and volume
  updates back to the sender.

The code is plain ES5 because older TVs (Tizen 3/4) run Node.js 4.4.3 and an
old Chromium.
