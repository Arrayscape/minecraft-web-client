/* eslint-disable */
// Runs *inside* the service worker: workbox's generated SW pulls this in via
// importScripts (see generateSW in rsbuild.config.ts).
//
// Why this exists. A deploy replaces the service worker, but the page that
// triggered the replacement was already served from the *old* precache, so it
// keeps running superseded code until something navigates it. Every page-side
// remedy has the same flaw — it ships in the new bundle, which is exactly the
// bundle the stale page is not running. Only the new service worker is fetched
// regardless of what the page is executing, so only the new service worker can
// fix a page that predates the fix.
//
// The risk that makes this delicate is reloading someone mid-game. So we ask
// first, and a page that does not answer is navigated. That default is safe in
// both directions:
//
//   - A bundle new enough to answer tells us truthfully whether it is in a game.
//   - A bundle too old to answer cannot be in a game against a proxy that
//     requires the resume handshake — it cannot open a session at all.
//
// Silence therefore means "old code, no session to lose", which is precisely the
// page we most want to replace.

// Generous on purpose. A page in a game answers late when the main thread is
// busy meshing chunks, and treating that jank as silence would navigate a player
// out of a live session — the one outcome this must never produce.
const ASK_TIMEOUT_MS = 2000

// Substituted at build time (see rsbuild.config.ts). Lets the sweep below tell a
// client that is merely uncontrolled from one that is actually out of date.
const BUILD_VERSION = '__MWC_BUILD_VERSION__'

// This worker decides, on its own, to navigate someone's tab. When it declines
// to, or fails to, that has to be visible: the first attempt swallowed every
// error and left no way to tell "never activated" from "activated and found no
// clients" from "navigate() threw".
const log = (...args) => {
  try {
    console.log('[sw-reload]', ...args)
  } catch { /* console may be gone during teardown */ }
}

const askClient = async (client) => {
  return new Promise(resolve => {
    let settled = false
    const finish = (busy) => {
      if (settled) return
      settled = true
      resolve(busy)
    }

    let channel
    try {
      channel = new MessageChannel()
      channel.port1.onmessage = (event) => { finish(event.data) }
      client.postMessage({ type: 'MWC_CAN_RELOAD' }, [channel.port2])
    } catch (err) {
      log('cannot ask client; treating as old bundle', err)
      finish(undefined) // see the note above on what silence means
      return
    }

    setTimeout(() => { finish(undefined) }, ASK_TIMEOUT_MS)
  })
}

// Navigations are deliberately NOT intercepted here.
//
// An earlier version served them network-first so a deploy would be picked up on
// the first page open rather than the second. It worked, and it cost too much:
// every in-app `location.reload()` — which is what "Disconnect & Reset" does
// (flyingSquidUtils.disconnect) — went through a fresh network fetch of the
// shell raced against a timeout, so leaving a game took seconds instead of being
// instant. It also broke on redirects: respondWith throws TypeError when handed
// a redirected response for a navigation, and `location = /play` is a 302.
//
// Letting the precache answer navigations restores that speed. The cost is that
// a new build lands one navigation later than it could, which the sweep below
// already handles.

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    // Take control first: navigate() only works on clients this worker controls,
    // and claiming is what makes the reload land on the new precache rather than
    // the one we are replacing.
    log('activating')
    try {
      await self.clients.claim()
    } catch (err) {
      log('claim failed', err) // workbox's clientsClaim may have done it already
    }

    let clients = []
    try {
      // includeUncontrolled matters: matchAll defaults to clients this worker
      // already controls, and immediately after claim() the pages being taken
      // over may not be listed yet. Without it the sweep finds nobody, navigates
      // nobody, and returns quietly having done nothing — which is exactly what
      // it appeared to do.
      clients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
    } catch (err) {
      log('matchAll failed', err)
      return
    }
    log(`found ${clients.length} window client(s)`)

    await Promise.all(clients.map(async (client) => {
      // Only tabs the user is actually looking at.
      //
      // Navigating a background tab is not free: same-origin tabs share a
      // renderer process, so reloading one runs a full app startup — atlas
      // generation, workers, asset decoding — on the same main thread as a game
      // running in another tab. Measured effect: framerate into the 20s, chunk
      // meshing stalling, and disconnect's location.reload() taking seconds.
      // The tab being repaired was not even the one that suffered.
      //
      // A hidden stale tab costs nothing by staying stale: it cannot hold a
      // session against a proxy that requires the resume handshake, and it will
      // be caught by a later activation once it is visible, or by any navigation
      // of its own.
      if (client.visibilityState && client.visibilityState !== 'visible') {
        log('client is in the background; leaving it alone', client.url)
        return
      }

      let reply
      try {
        reply = await askClient(client)
      } catch (err) {
        log('ask failed; treating as old bundle', err)
        reply = undefined
      }

      // Already running what this worker is here to install. Navigating it would
      // reload a page for no reason — which is what the first version of this
      // did on every fresh registration, including a user's very first visit,
      // where there is by definition nothing stale to replace.
      if (reply && reply.version && reply.version === BUILD_VERSION) {
        log('client is already current; leaving it alone', client.url)
        return
      }

      if (reply && reply.busy) {
        log('client is busy; leaving it alone', client.url)
        return
      }

      // No answer means a bundle that predates this protocol. Every proxy in
      // production requires the resume handshake, so such a bundle cannot hold a
      // session — it is refused at the handshake — and therefore has no game to
      // interrupt. That is precisely the client worth replacing, and navigating
      // it is the whole point of this sweep.
      //
      // What made this dangerous before was not the rule but a broken reply
      // path: navigator.serviceWorker.startMessages() was never called, so *no*
      // client could answer and every one of them looked old. The rule is only
      // as safe as the channel it depends on.
      try {
        log('navigating', client.url)
        await client.navigate(client.url)
      } catch (err) {
        log('navigate failed', client.url, err)
      }
    }))
    log('done')
  })())
})
