import { isCypress } from './standaloneUtils'

// Auto-update after deploy: workbox builds the SW with skipWaiting +
// clientsClaim, so a new SW takes over open pages as soon as it activates.
// When that happens the new SW will serve fresh index.html and bundle
// hashes — but the page that's currently rendered is still running the
// old bundle. Reload once on first controllerchange so the user picks up
// the new build automatically.
//
// The `refreshing` guard is required because controllerchange can fire
// during the initial registration on a fresh visit (no prior SW), and we
// don't want to bounce-reload mid-page-load in that case. We also avoid
// reloading if there was no controller before — that's the first-install
// case, not an update.
let reloadingForSWUpdate = false
const wireUpdateReload = () => {
  if (!navigator.serviceWorker.controller) return // first install, not an update
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (reloadingForSWUpdate) return
    reloadingForSWUpdate = true
    window.location.reload()
  })
}

// might not resolve at all
export const registerServiceWorker = async () => {
  if (!('serviceWorker' in navigator) || process.env.SINGLE_FILE_BUILD) return
  if (process.env.DISABLE_SERVICE_WORKER) return
  if (!isCypress() && process.env.NODE_ENV !== 'development') {
    return new Promise<void>(resolve => {
      window.addEventListener('load', async () => {
        wireUpdateReload()
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
