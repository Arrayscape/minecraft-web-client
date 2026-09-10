import { subscribe } from 'valtio'
import { isCypress } from './standaloneUtils'
import { miscUiState } from './globalState'

let reloadArmed = false

/**
 * Reload once the player is out of the game.
 *
 * The worker asked to navigate this tab and we said no because a session was
 * running. That answer has to come with a promise to do it later, or a player
 * who stays in one game keeps the superseded bundle indefinitely.
 */
const reloadWhenGameEnds = () => {
  if (reloadArmed) return
  reloadArmed = true
  const stop = subscribe(miscUiState, () => {
    if (miscUiState.gameLoaded) return
    stop()
    location.reload()
  })
}

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
 */
export const listenForReloadRequests = () => {
  if (!('serviceWorker' in navigator)) return
  navigator.serviceWorker.addEventListener('message', (event: MessageEvent) => {
    if (event.data?.type !== 'MWC_CAN_RELOAD') return
    const port = event.ports?.[0]
    if (!port) return
    if (miscUiState.gameLoaded) {
      port.postMessage('busy')
      reloadWhenGameEnds()
    } else {
      port.postMessage('idle')
    }
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
