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

// How long a navigation waits for the network before the precached shell is
// served instead. Long enough for a bad connection to answer, short enough that
// a dead one does not strand the player on a blank page.
const NAVIGATION_TIMEOUT_MS = 4000

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

// Navigations go to the network, always, with cache only as a fallback.
//
// This is what makes a deploy visible on the *first* page open rather than after
// one. Workbox precaches index.html and, via its directoryIndex default, answers
// a request for "/" out of that precache — so the old shell is served without
// the network being consulted at all, and the only thing that could notice a new
// release is the incoming worker, which cannot act until it has downloaded the
// entire app. That is a lag measured in however long the bundle takes.
//
// Fetching the shell instead costs a few KB against a no-cache endpoint, and it
// settles the question immediately: the fresh HTML names fresh hashed chunks,
// none of which are in the old precache, so they miss every precache route and
// load from the network too. The page is on the new build before the new worker
// has finished installing.
//
// Registered here, ahead of workbox's own routes, because the first listener to
// call respondWith owns the request.
self.addEventListener('fetch', (event) => {
  const request = event.request
  if (request.mode !== 'navigate') return

  event.respondWith((async () => {
    const cachedShell = async () => {
      // ignoreSearch because workbox stores revisioned entries under a
      // __WB_REVISION__ query, so an exact match on './index.html' misses.
      const cached = await caches.match('./index.html', { ignoreSearch: true })
      return cached ?? Response.error()
    }

    let timer
    try {
      // Bounded, because these are venue networks: a link that is merely slow
      // rather than down would otherwise hold the page on a blank screen for as
      // long as it felt like. Falling back to the precached shell keeps the
      // client usable, and the incoming worker still navigates it once it has
      // installed, so a stale shell here self-corrects rather than sticking.
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => { reject(new Error('navigation fetch timed out')) }, NAVIGATION_TIMEOUT_MS)
      })
      const response = await Promise.race([fetch(request), timeout])
      // A reply is not the same as a working one. Captive portals and failing
      // edges answer promptly with something useless, and handing that to the
      // player instead of the app we already hold would be a worse outcome than
      // being offline outright.
      if (!response || !response.ok) {
        throw new Error(`navigation returned ${response ? response.status : 'nothing'}`)
      }
      return response
    } catch (err) {
      log('serving the cached shell', err)
      return cachedShell()
    } finally {
      clearTimeout(timer)
    }
  })())
})

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

      // A busy client reloads itself when the game ends; see serviceWorker.ts.
      if (reply && reply.busy) {
        log('client is in a game; it will reload itself when the game ends', client.url)
        return
      }
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
