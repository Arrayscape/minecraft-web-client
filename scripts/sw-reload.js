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

const askIfBusy = async (client) => {
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
      channel.port1.onmessage = (event) => { finish(event.data === 'busy') }
      client.postMessage({ type: 'MWC_CAN_RELOAD' }, [channel.port2])
    } catch {
      finish(false) // cannot be asked; see the note above on what silence means
      return
    }

    setTimeout(() => { finish(false) }, ASK_TIMEOUT_MS)
  })
}

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    // Take control first: navigate() only works on clients this worker controls,
    // and claiming is what makes the reload land on the new precache rather than
    // the one we are replacing.
    try {
      await self.clients.claim()
    } catch { /* workbox's clientsClaim may have done it already */ }

    let clients = []
    try {
      clients = await self.clients.matchAll({ type: 'window' })
    } catch {
      return
    }

    await Promise.all(clients.map(async (client) => {
      let busy = false
      try {
        busy = await askIfBusy(client)
      } catch {
        busy = false
      }
      // A busy client reloads itself when the game ends; see serviceWorker.ts.
      if (busy) return
      try {
        await client.navigate(client.url)
      } catch { /* the tab may have gone away mid-ask */ }
    }))
  })())
})
