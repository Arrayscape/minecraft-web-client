import fs from 'fs'
import { motion, AnimatePresence } from 'framer-motion'
import { proxy, subscribe, useSnapshot } from 'valtio'
import { useEffect, useState } from 'react'
import { activeModalStack, miscUiState, openOptionsMenu, showModal } from '../globalState'
import { openGithub } from '../utils'
import { setLoadingScreenStatus } from '../appStatus'
import { openFilePicker, copyFilesAsync, mkdirRecursive, openWorldDirectory, removeFileRecursiveAsync } from '../browserfs'

import MainMenu from './MainMenu'

const isMainMenu = () => {
  return activeModalStack.length === 0 && !miscUiState.gameLoaded
}

const refreshApp = async (failedUpdate = false) => {
  try {
    const registration = await navigator.serviceWorker.getRegistration()
    if (registration) {
      // First, disconnect all clients
      const clients = await window.clients?.matchAll() || []
      await Promise.all(clients.map(client => client.postMessage('SKIP_WAITING')))

      // Force the waiting service worker to become active
      if (registration.waiting) {
        registration.waiting.postMessage('SKIP_WAITING')
      }

      // Add timeout to prevent infinite waiting
      const unregisterPromise = registration.unregister()
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('SW unregister timeout')), 3000)
      })

      await Promise.race([unregisterPromise, timeoutPromise])
        .catch(err => {
          console.warn('SW unregister error:', err)
          if (isMainMenu()) {
            alert('Failed to unregister SW: ' + err)
          }
        })
    }

    if (failedUpdate) {
      await new Promise(resolve => { setTimeout(resolve, 2000) })
    }

    if (!isMainMenu()) return

    if (failedUpdate) {
      // sessionStorage values are always strings — `= false` stores the
      // string "false" which is truthy on read and would trap the version
      // check (line ~98) in an endless refreshApp loop. Remove it so the
      // next page load reads `null` (falsy) and breaks out.
      sessionStorage.removeItem('justReloaded')
      // try to force bypass cache
      location.search = '?update=true'
    } else {
      window.justReloaded = true
      sessionStorage.justReloaded = 'true'
      window.location.reload()
    }
  } catch (err) {
    console.error('Failed to refresh app:', err)
    if (!isMainMenu()) {
      alert('Critical error on refreshApp: ' + err)
      // Fallback to force reload if something goes wrong
      window.location.reload()
    }
  }
}

export const mainMenuState = proxy({
  serviceWorkerLoaded: false,
})

// todo clean
let disableAnimation = false
export default () => {
  const haveModals = useSnapshot(activeModalStack).length
  const { gameLoaded, fsReady, appConfig, singleplayerAvailable } = useSnapshot(miscUiState)

  const noDisplay = haveModals || gameLoaded || !fsReady

  useEffect(() => {
    if (noDisplay && fsReady) disableAnimation = true
  }, [noDisplay])

  const [versionStatus, setVersionStatus] = useState('')
  const [versionTitle, setVersionTitle] = useState('')

  useEffect(() => {
    if (process.env.SINGLE_FILE_BUILD_MODE) {
      setVersionStatus('(single file build)')
    } else if (process.env.NODE_ENV === 'development') {
      setVersionStatus('(dev)')
    } else {
      // Absolute, and explicitly uncached. Relative resolves to
      // /play/version.txt under a magic-link URL, which matches the /play/
      // location rather than `location = /version.txt`, so the probe misses its
      // no-cache header and can itself be answered from cache — a freshness
      // check served stale reports whatever it last saw.
      fetch('/version.txt', { cache: 'no-store' }).then(async (f) => {
        if (f.status === 404) return
        const contents = (await f.text()).trim()
        const isLatest = contents === process.env.BUILD_VERSION

        // A mismatch means this tab is running superseded code. Refresh once per
        // remote version: enough to pick it up, and bounded, so a bundle that
        // somehow cannot update cannot spin. Previously this only acted when
        // sessionStorage.justReloaded happened to be set, so an ordinary first
        // load after a deploy simply displayed "new version available" and went
        // on running the old code.
        if (!isLatest && sessionStorage.reloadedFor !== contents) {
          sessionStorage.reloadedFor = contents
          setVersionStatus('(updating, wait)')
          void refreshApp(true)
          return
        }
        if (!isLatest && sessionStorage.justReloaded) {
          setVersionStatus('(force reloading, wait)')
          void refreshApp(true)
          return
        }
        const upStatus = () => {
          setVersionStatus(`(${isLatest ? 'latest' : 'new version available'}${mainMenuState.serviceWorkerLoaded ? ', Downloaded' : ''})`)
        }
        subscribe(mainMenuState, upStatus)
        upStatus()
        setVersionTitle(`Loaded: ${process.env.BUILD_VERSION}. Remote: ${contents}`)
      }, () => {
        setVersionStatus('(offline)')
      })
    }
  }, [])

  const mapsProviderUrl = appConfig?.mapsProvider && new URL(appConfig?.mapsProvider)
  if (mapsProviderUrl && location.origin !== 'https://mcraft.fun') {
    mapsProviderUrl.searchParams.set('to', location.href)
  }

  return (
    <AnimatePresence>
      {!noDisplay && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{
            duration: disableAnimation ? 0 : 0.1,
            ease: 'easeInOut'
          }}
        >
          <MainMenu
            singleplayerAvailable={singleplayerAvailable}
            connectToServerAction={() => showModal({ reactType: 'serversList' })}
            singleplayerAction={async () => {
              showModal({ reactType: 'singleplayer' })
            }}
            githubAction={() => openGithub()}
            optionsAction={() => openOptionsMenu('main')}
            bottomRightLinks={process.env.MAIN_MENU_LINKS}
            openFileAction={e => {
              if (!!window.showDirectoryPicker && !e.shiftKey) {
                void openWorldDirectory()
              } else {
                openFilePicker()
              }
            }}
            mapsProvider={mapsProviderUrl?.toString()}
            versionStatus={versionStatus}
            versionTitle={versionTitle}
            onVersionStatusClick={async () => {
              setVersionStatus('(reloading)')
              await refreshApp()
            }}
            onVersionTextClick={async () => {
              openGithub(process.env.RELEASE_LINK)
            }}
            versionText={process.env.RELEASE_TAG}
          />
        </motion.div>
      )}
    </AnimatePresence>
  )
}
