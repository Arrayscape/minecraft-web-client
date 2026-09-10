import { subscribe } from 'valtio'
import { isCypress } from './standaloneUtils'
import { activeModalStack, miscUiState } from './globalState'

/**
 * Answer the service worker when it asks whether this tab can be navigated.
 *
 * A deploy leaves the page that triggered it running the previous bundle out of
 * the old precache, and nothing in that bundle can fix it — the fix always ships
 * in the *new* one. The incoming worker can, so it navigates its clients on
 * activation, and asks first so it never does that to someone mid-game.
 *
 * Not answering is itself an answer: it means a bundle older than this protocol,
 * which cannot hold a session against a proxy that requires the resume
 * handshake, so it has no game to lose.
 *
 * A busy tab is simply left alone. It used to promise to reload itself once the
 * game ended, which raced disconnect()'s own location.reload() — two navigations
 * for one click. It picks up the new build on its next navigation like anything
 * else.
 */
export const listenForReloadRequests = () => {
  if (!('serviceWorker' in navigator)) return
  navigator.serviceWorker.addEventListener('message', (event: MessageEvent) => {
    if (event.data?.type !== 'MWC_CAN_RELOAD') return
    const port = event.ports?.[0]
    if (!port) return
    // "Busy" cannot mean only "in a game". gameLoaded is set *after* chunks
    // finish (index.ts, following waitForChunks), so a player who is connecting
    // — the worst possible moment to navigate — reports itself idle. Reloading
    // them there is what left the world blank with the chunk progress stuck at
    // 0%: the page was thrown away mid-load.
    //
    // Anything on the modal stack counts, which includes the app-status screen
    // shown throughout connecting and loading. Only a bare main menu is safe.
    const busy = miscUiState.gameLoaded || activeModalStack.length > 0
    // The version lets the worker tell "this page is stale" from "this page is
    // already what I am installing", so a current tab is never reloaded at all.
    port.postMessage({ busy, version: process.env.BUILD_VERSION })
  })
}

// might not resolve at all
export const registerServiceWorker = async () => {
  if (!('serviceWorker' in navigator) || process.env.SINGLE_FILE_BUILD) return
  if (process.env.DISABLE_SERVICE_WORKER) return
  if (!isCypress() && process.env.NODE_ENV !== 'development') {
    return new Promise<void>(resolve => {
      window.addEventListener('load', async () => {
        await navigator.serviceWorker.register('./service-worker.js').then(registration => {
          console.log('SW registered:', registration)
          resolve()
        }).catch(registrationError => {
          console.log('SW registration failed:', registrationError)
        })
      })
    })
  } else {
    // force unregister service worker in development mode
    const registrations = await navigator.serviceWorker.getRegistrations()
    for (const registration of registrations) {
      await registration.unregister() // eslint-disable-line no-await-in-loop
    }
    if (registrations.length) {
      location.reload()
    }
  }
}
