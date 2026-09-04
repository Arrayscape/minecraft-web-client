#!/usr/bin/env node
// Measure how long a Minecraft server tolerates a silent client before kicking.
//
// This is the "kick window" from the resume plan (MWCProxy/websocket-resume.md).
// It bounds how long the proxy can hold a TCP connection open while a player's
// WebSocket is away — and therefore the longest outage a byte-offset resume can
// hide without the proxy having to answer keep-alives itself.
//
// It connects directly to the Minecraft server, NOT through MWCProxy. Under
// phase 1 behaviour the proxy closes the TCP as soon as the WebSocket ends, so
// it cannot hold a silent connection open; that is what phase 2 adds. Measuring
// standalone sidesteps the ordering problem.
//
// Method: join normally, then go completely silent — no keep-alive responses and
// no movement — and time how long until the server ends the connection. Silence
// is total on purpose: that is exactly what the server sees when a player's
// browser has dropped and the proxy is holding their TCP open.
//
// Usage:
//   node scripts/measureKickWindow.mjs
//   MC_HOST=localhost MC_PORT=25565 MC_VERSION=1.21.4 node scripts/measureKickWindow.mjs
//
// The number is server-software and version specific. Run it against the same
// build production uses, or the answer does not transfer.

import mineflayer from 'mineflayer'

const HOST = process.env.MC_HOST ?? 'localhost'
const PORT = Number(process.env.MC_PORT ?? 25565)
const VERSION = process.env.MC_VERSION || false // false = auto-detect
const USERNAME = process.env.MC_USERNAME ?? 'KickWindowProbe'
const GIVE_UP_AFTER_MS = Number(process.env.GIVE_UP_AFTER_MS ?? 15 * 60 * 1000)

const started = Date.now()
const secs = (ms) => (ms / 1000).toFixed(1)

console.log(`[kick-window] connecting to ${HOST}:${PORT} as ${USERNAME}${VERSION ? ` (${VERSION})` : ' (auto version)'}`)

const bot = mineflayer.createBot({
  host: HOST,
  port: PORT,
  username: USERNAME,
  auth: 'offline',
  ...(VERSION ? { version: VERSION } : {}),
  // Disables both the keep-alive response and minecraft-protocol's own
  // client-side timeout, so the only thing that can end this session is the
  // server deciding to. See minecraft-protocol/src/client/keepalive.js:4-5.
  keepAlive: false,
})

let silentSince = null
let suppressed = 0
let keepAlivesSeen = 0
let finished = false

const finish = (how, detail) => {
  if (finished) return
  finished = true
  clearTimeout(giveUp)

  if (silentSince === null) {
    console.log(`\n[kick-window] ended before going silent (${how}): ${detail ?? ''}`)
    console.log('[kick-window] no measurement — check the version and that the server is reachable')
    process.exit(1)
  }

  const elapsed = Date.now() - silentSince
  console.log('')
  console.log('─'.repeat(64))
  console.log(`  KICK WINDOW: ${secs(elapsed)}s of total silence before disconnect`)
  console.log('─'.repeat(64))
  console.log(`  ended by       ${how}`)
  if (detail) console.log(`  reason         ${String(detail).slice(0, 200)}`)
  console.log(`  keep-alives    ${keepAlivesSeen} received, 0 answered`)
  console.log(`  packets held   ${suppressed} suppressed while silent`)
  console.log(`  server         ${HOST}:${PORT}${bot.version ? ` (${bot.version})` : ''}`)
  console.log('')
  console.log('  This bounds how long a resume can hide an outage before the')
  console.log('  server ends the session on its own.')
  console.log('')
  process.exit(0)
}

bot.on('kicked', (reason) => finish('kicked', typeof reason === 'string' ? reason : JSON.stringify(reason)))
bot.on('end', (reason) => finish('end', reason))
bot.on('error', (err) => {
  if (finished) return
  if (silentSince === null) {
    console.error('[kick-window] error before going silent:', err.message)
    process.exit(1)
  }
  finish('error', err.message)
})

bot.on('spawn', () => {
  if (silentSince !== null) return // respawn, not a fresh join

  // Count keep-alives so the output can show the server was actively asking.
  bot._client.on('keep_alive', () => { keepAlivesSeen++ })

  // Go completely silent. Physics keeps running on a local timer and would
  // otherwise keep writing position packets every tick, which is not what a
  // dropped player looks like — from the server's side, that client is gone.
  bot.physicsEnabled = false
  const realWrite = bot._client.write.bind(bot._client)
  bot._client.write = (name, params) => {
    if (finished) return realWrite(name, params)
    suppressed++
    // Deliberately dropped.
  }

  silentSince = Date.now()
  console.log(`[kick-window] spawned after ${secs(Date.now() - started)}s — going silent now`)
  console.log('[kick-window] waiting for the server to give up (Ctrl-C to abort)...')

  const tick = setInterval(() => {
    if (finished) { clearInterval(tick); return }
    process.stdout.write(`\r[kick-window] silent for ${secs(Date.now() - silentSince)}s, ${keepAlivesSeen} keep-alives ignored   `)
  }, 1000)
})

// minecraft-protocol surfaces socket-level failures (ECONNREFUSED, version
// mismatch) as uncaught rather than through bot.on('error'), and a raw stack
// trace is a poor answer to "is the server reachable".
process.on('uncaughtException', (err) => {
  if (finished) return
  if (silentSince === null) {
    console.error(`\n[kick-window] could not establish a session with ${HOST}:${PORT}`)
    console.error(`[kick-window] ${err.message}`)
    console.error('[kick-window] check the server is up, offline-mode, and that MC_VERSION matches')
    process.exit(1)
  }
  finish('error', err.message)
})

const giveUp = setTimeout(() => {
  if (finished) return
  console.log(`\n[kick-window] still connected after ${secs(GIVE_UP_AFTER_MS)}s — the server may not time out silent clients at all.`)
  console.log('[kick-window] that would mean the kick window is not a constraint on resume.')
  process.exit(0)
}, GIVE_UP_AFTER_MS)
