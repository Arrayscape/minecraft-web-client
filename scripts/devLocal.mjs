#!/usr/bin/env node
// Run the client against a locally running MWCProxy.
//
// The client will NOT use MWCProxy on its own. `pnpm start` boots the bundled
// node proxy (server.js + net-browserify) and serves a config.json with
// defaultProxy: "", so getCurrentProxy() returns undefined and net-browserify
// falls back to the page's own origin. You end up exercising the node proxy and
// none of the Go one — which looks identical until you notice the proxy log is
// empty.
//
// So: skip the node proxy entirely, and hand the browser an explicit ?proxy=.
//
// rsbuild serves on 3000 (its default; server.js's 8080 is a different thing
// and is not started here). If 3000 is taken rsbuild picks another and says so
// in its own output — pass PORT= to match.
//
// Usage:
//   pnpm dev-local                              # client :3000, proxy :8081
//   MC_HOST=192.168.1.5 pnpm dev-local          # Minecraft server elsewhere
//   USERNAME_OVERRIDE=Tad pnpm dev-local
//   PORT=3001 pnpm dev-local                    # rsbuild picked another port
//   MWC_PROXY_URL=http://localhost:9000 pnpm dev-local
//   NO_OPEN=1 pnpm dev-local                    # don't launch a browser

import { spawn } from 'child_process'

const PROXY_URL = process.env.MWC_PROXY_URL ?? 'http://localhost:8081'
const CLIENT_PORT = process.env.PORT ?? '3000'
const CLIENT_ORIGIN = `http://localhost:${CLIENT_PORT}`
const MC_HOST = process.env.MC_HOST ?? 'localhost'
const USERNAME = process.env.USERNAME_OVERRIDE ?? 'DevPlayer'

// `ip` is not optional here, despite `proxy` alone being enough to preload the
// form. index.ts:1159 opens the edit-server modal for either param, but does it
// with showModal() directly rather than through setServerEditScreen — so
// serverEditScreen stays null and ServersListProvider's onConfirm returns at its
// `if (!serverEditScreen)` guard. Save silently does nothing.
//
// Supplying `ip` switches the screen to its Connect button, which runs
// onQsConnect instead and has no such guard.
const CLIENT_URL = `${CLIENT_ORIGIN}/?ip=${MC_HOST}&proxy=${PROXY_URL}&username=${USERNAME}`

const banner = (lines) => {
  const width = Math.max(...lines.map(l => l.length)) + 4
  console.log('\n' + '─'.repeat(width))
  for (const l of lines) console.log(`  ${l}`)
  console.log('─'.repeat(width) + '\n')
}

const checkProxy = async () => {
  try {
    const res = await fetch(`${PROXY_URL}/health`, { signal: AbortSignal.timeout(1500) })
    return res.ok
  } catch {
    return false
  }
}

const waitForReady = async (origin) => {
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(origin, { signal: AbortSignal.timeout(1000) })
      if (res.ok) return true
    } catch {}
    await new Promise(r => setTimeout(r, 500))
  }
  return false
}

const openBrowser = (url) => {
  const opener = process.platform === 'darwin' ? 'open'
    : process.platform === 'win32' ? 'start'
      : 'xdg-open'
  spawn(opener, [url], { stdio: 'ignore', detached: true, shell: process.platform === 'win32' }).unref()
}

const main = async () => {
  const proxyUp = await checkProxy()

  banner([
    `client   ${CLIENT_URL}`,
    `proxy    ${PROXY_URL}   ${proxyUp ? '[up]' : '[NOT RESPONDING]'}`,
    ...(proxyUp ? [] : ['', 'Start the proxy first:  cd ../MWCProxy && make run']),
    '',
    'If rsbuild reports a port other than ' + CLIENT_PORT + ', rerun with PORT=<that port>.',
    '',
    'Click Connect on the screen that opens. Do not use Save — on a URL',
    'with query params it is a no-op (index.ts:1159 opens the modal without',
    'setting serverEditScreen, so onConfirm returns early).',
    '',
    'The ?proxy= parameter is required — without it the client talks to',
    'its own origin, not MWCProxy. The port field is ignored; the proxy',
    'uses MWC_PROXY_MC_PORT.',
    '',
    'The Minecraft server needs online-mode=false: with no auth flow the',
    'client refuses to join an online-mode server.',
  ])

  // Same processes as `pnpm start2` — rsbuild plus the mesher watcher, and
  // deliberately not server.js, since MWCProxy is the proxy here.
  const child = spawn('pnpm', ['run-p', 'dev-rsbuild', 'watch-mesher'], {
    stdio: 'inherit',
  })
  child.on('exit', code => process.exit(code ?? 0))

  if (process.env.NO_OPEN) return

  if (await waitForReady(CLIENT_ORIGIN)) {
    openBrowser(CLIENT_URL)
    console.log(`\n[dev-local] opened ${CLIENT_URL}\n`)
  } else {
    console.log(`\n[dev-local] ${CLIENT_ORIGIN} not answering — check rsbuild's port above, then open:\n  ${CLIENT_URL}\n`)
  }
}

void main()
