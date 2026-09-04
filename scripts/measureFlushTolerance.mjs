#!/usr/bin/env node
// Measure what a Minecraft server does when a client's queued traffic arrives
// all at once.
//
// This is the "flush tolerance" from the resume plan
// (MWCProxy/websocket-resume.md). When a player's WebSocket drops, the client
// keeps playing and its packets queue up; on resume they are delivered in one
// burst. Per-tick movement validation exists to catch exactly that shape, so the
// question is how large a burst the server accepts before it corrects or kicks.
//
// Velocity's packet limiter is NOT the risk here — production has
// packets-per-second and bytes-per-second disabled, and the surviving
// decompressed-bytes check has 100x+ headroom. The target is Paper's movement
// validation at the backend, so run this against the production Paper version.
//
// Method: join, walk continuously, then intercept the socket write path so that
// nothing reaches the server for N seconds while the client keeps playing.
// Release the whole buffer at once and watch for a kick or a rubber-band.
//
// Buffering is at the socket, below the cipher, matching how the real design
// works: a byte-level queue cannot reorder or drop anything, so the server sees
// exactly the packets it would have seen, just late and all together.
//
// N must stay under the kick window (measured: 30s) or the queued keep-alive
// responses arrive too late and the server drops the connection for silence
// rather than for the burst — a different result wearing the same clothes.
//
// Usage:
//   HOLD_SECONDS=10 node scripts/measureFlushTolerance.mjs
//
//   for n in 2 5 10 15 20 25; do
//     HOLD_SECONDS=$n node scripts/measureFlushTolerance.mjs
//   done

import mineflayer from 'mineflayer'

const HOST = process.env.MC_HOST ?? 'localhost'
const PORT = Number(process.env.MC_PORT ?? 25565)
const VERSION = process.env.MC_VERSION || false
const USERNAME = process.env.MC_USERNAME ?? 'FlushProbe'
const HOLD_SECONDS = Number(process.env.HOLD_SECONDS ?? 10)
// "x y z" of open, flat ground. Requires the probe username to be opped.
// Minecraft respawns a returning player where they logged out, so without this
// each run of a sweep starts wherever the last one finished — and the bot ends
// up progressively more wedged against terrain.
const START_AT = process.env.START_AT || ''
// Compass direction to walk. Yaw in degrees, Minecraft convention:
// south=0, west=90, north=180, east=270.
const WALK_YAW = process.env.WALK_YAW === undefined ? null : Number(process.env.WALK_YAW)
const SETTLE_MS = Number(process.env.SETTLE_MS ?? 8000)

if (HOLD_SECONDS >= 30) {
  console.error(`[flush] HOLD_SECONDS=${HOLD_SECONDS} is at or past the 30s kick window.`)
  console.error('[flush] The connection would drop for silence, not for the burst. Use less than 30.')
  process.exit(1)
}

const secs = (ms) => (ms / 1000).toFixed(1)
const round = (v) => Math.round(v * 10) / 10
const posOf = (p) => p ? `(${round(p.x)}, ${round(p.y)}, ${round(p.z)})` : '?'
const dist = (a, b) => Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z)

console.log(`[flush] connecting to ${HOST}:${PORT} as ${USERNAME}, hold ${HOLD_SECONDS}s`)

const bot = mineflayer.createBot({
  host: HOST,
  port: PORT,
  username: USERNAME,
  auth: 'offline',
  ...(VERSION ? { version: VERSION } : {}),
})

let held = []
let heldBytes = 0
let heldPackets = 0
let posAtSpawn = null
let posBeforeFlush = null
let posAfterHold = null
// Split by phase. A forcedMove arriving DURING the hold cannot have been caused
// by the burst, because nothing has been sent yet — so it is proof that
// something else is moving the player. Only post-flush corrections count as a
// result.
let forcedMovesDuringHold = 0
let forcedMovesAfterFlush = 0
let flushedAt = null
let finished = false
let phase = 'connecting'

const report = (outcome, detail) => {
  if (finished) return
  finished = true

  const verdict =
    outcome === 'kicked' || outcome === 'disconnected' ? 'REJECTED'
      : forcedMovesAfterFlush > 0 ? 'CORRECTED'
        : 'ACCEPTED'

  const teleportedIn = posAtSpawn && posBeforeFlush && dist(posAtSpawn, posBeforeFlush) > 50
  const contaminated = forcedMovesDuringHold > 0 || teleportedIn
  const movedDuringHold = posBeforeFlush && posAfterHold ? dist(posBeforeFlush, posAfterHold) : 0
  // A burst with no movement in it does not exercise movement validation, so
  // "accepted" would mean nothing. Distinguish that from a real result.
  const noMovement = phase === 'settling' && movedDuringHold < 1

  console.log('\n' + '─'.repeat(66))
  console.log(`  FLUSH TOLERANCE @ ${HOLD_SECONDS}s hold:  ${verdict}`)
  console.log('─'.repeat(66))
  console.log(`  buffered        ${heldPackets} packets, ${heldBytes} bytes`)
  console.log(`  burst rate      ${round(heldBytes / HOLD_SECONDS)} B/s equivalent, delivered at once`)
  console.log(`  forced moves    ${forcedMovesAfterFlush} after flush${forcedMovesAfterFlush ? '   <-- server corrected our position' : ''}`)
  if (forcedMovesDuringHold) console.log(`                  ${forcedMovesDuringHold} DURING hold (not caused by us)`)
  if (posBeforeFlush && posAfterHold) {
    console.log(`  position        ${posOf(posBeforeFlush)} at hold start`)
    console.log(`                  ${posOf(posAfterHold)} after flush`)
    console.log(`  moved           ${round(dist(posBeforeFlush, posAfterHold))} blocks during the hold`)
  }
  console.log(`  outcome         ${outcome}${detail ? ` — ${String(detail).slice(0, 160)}` : ''}`)
  console.log('')

  if (contaminated) {
    console.log('  !! RESULT NOT TRUSTWORTHY — something else is moving the player.')
    if (teleportedIn) {
      console.log(`     Teleported ${round(dist(posAtSpawn, posBeforeFlush))} blocks between spawn and hold start.`)
    }
    if (forcedMovesDuringHold) {
      console.log('     The server corrected our position while we were sending nothing,')
      console.log('     so those corrections cannot be a reaction to the burst.')
    }
    console.log('')
    console.log('     Use a clean world: no plugins that teleport or reposition players,')
    console.log('     no spawn protection, somewhere flat and walkable.')
    console.log('')
    process.exit(2)
  }

  if (noMovement) {
    console.log('  !! INCONCLUSIVE — the player barely moved during the hold, so the')
    console.log(`     burst carried almost no movement (${round(movedDuringHold)} blocks, ${round(heldBytes / HOLD_SECONDS)} B/s`)
    console.log('     is the idle packet rate). Movement validation was never exercised.')
    console.log('')
    console.log('     The bot is probably wedged against terrain. Re-run somewhere open,')
    console.log('     or teleport it to flat ground first.')
    console.log('')
    process.exit(3)
  }

  if (verdict === 'ACCEPTED') {
    console.log('  The server took the whole burst without complaint. A resume across')
    console.log(`  a ${HOLD_SECONDS}s gap would land cleanly. Try a longer hold.`)
  } else if (verdict === 'CORRECTED') {
    console.log('  The server accepted the connection but rubber-banded the player.')
    console.log('  Survivable, but this is where pacing the flush starts to earn its keep.')
  } else {
    console.log('  The server ended the connection. Check the server log for the reason:')
    console.log('  "moved too quickly" / "moved wrongly" points at movement validation.')
    console.log(`  This is the upper bound — resume must not queue ${HOLD_SECONDS}s of play.`)
  }
  console.log('')
  process.exit(0)
}

bot.on('kicked', (reason) => report('kicked', typeof reason === 'string' ? reason : JSON.stringify(reason)))
bot.on('end', (reason) => {
  if (phase === 'settling') report('disconnected', reason)
  else if (!finished) { console.error(`\n[flush] connection ended during ${phase}: ${reason}`); process.exit(1) }
})

// The server correcting our position is the softer failure mode, and the one
// that shows up before an outright kick as the burst grows.
bot.on('forcedMove', () => {
  if (phase === 'settling') {
    forcedMovesAfterFlush++
    console.log(`\n[flush] forcedMove -> server pulled us to ${posOf(bot.entity?.position)}`)
  } else if (phase === 'holding') {
    forcedMovesDuringHold++
  }
})

// Keep the player genuinely moving on an autogenerated world. Walking in a
// straight line wedges against the first tree or hillside, and a wedged bot
// sends only idle packets — which would make the whole measurement vacuous.
// Jumping clears most obstacles; swinging the yaw means a blocked direction is
// temporary rather than permanent.
const startMoving = (fixedDirection = false) => {
  bot.setControlState('forward', true)
  bot.setControlState('sprint', true)
  return setInterval(() => {
    // Jump clears small obstacles either way. Only swing the yaw when no clear
    // direction was given — on known-open ground, turning would walk us off it.
    bot.setControlState('jump', true)
    setTimeout(() => bot.setControlState('jump', false), 250)
    if (!fixedDirection) {
      bot.look(bot.entity.yaw + Math.PI / 5, 0, false).catch(() => {})
    }
  }, 1500)
}

bot.once('spawn', async () => {
  phase = 'walking'

  if (START_AT) {
    console.log(`[flush] at ${posOf(bot.entity.position)}, teleporting to ${START_AT}`)
    bot.chat(`/tp ${START_AT}`)
    // Let the teleport land before anything is measured. posAtSpawn is taken
    // afterwards on purpose: the teleport is ours, so it must not count as the
    // external repositioning the contamination check looks for.
    await new Promise(r => setTimeout(r, 1500))

    // Verify rather than assume. A /tp that silently fails — not opped, bad
    // syntax, unloaded chunk — leaves the bot walking from wherever it logged
    // out, which produces a confident-looking result measured on the wrong
    // ground.
    const [tx, ty, tz] = START_AT.trim().split(/\s+/).map(Number)
    const landed = bot.entity.position
    const off = dist({ x: tx, y: ty, z: tz }, landed)
    if (!Number.isFinite(off) || off > 3) {
      console.error(`\n[flush] teleport did not land: asked for (${tx}, ${ty}, ${tz}), at ${posOf(landed)} (${round(off)} blocks off)`)
      console.error(`[flush] check that ${USERNAME} is opped and the target chunk is loaded`)
      process.exit(4)
    }
    console.log(`[flush] landed at ${posOf(landed)}`)
  }

  if (WALK_YAW !== null) {
    await bot.look(WALK_YAW * Math.PI / 180, 0, true).catch(() => {})
    console.log(`[flush] facing yaw ${WALK_YAW}°`)
  }

  posAtSpawn = { ...bot.entity.position }
  console.log(`[flush] starting at ${posOf(posAtSpawn)} — walking to generate movement`)
  const mover = startMoving(WALK_YAW !== null)

  // Walk before holding, both so the buffer contains real movement rather than
  // spawn-settling packets and so we can confirm the bot actually moves here.
  await new Promise(r => setTimeout(r, 4000))
  const warmupMoved = dist(posAtSpawn, bot.entity.position)
  console.log(`[flush] warmup moved ${round(warmupMoved)} blocks`)

  const socket = bot._client.socket
  const realWrite = socket.write.bind(socket)

  posBeforeFlush = { ...bot.entity.position }
  phase = 'holding'
  console.log(`[flush] holding all output for ${HOLD_SECONDS}s from ${posOf(posBeforeFlush)}`)

  // Buffer below the cipher, exactly as the real queue does. Nothing is dropped
  // or reordered — the server will see every packet, just late.
  socket.write = (chunk, encoding, cb) => {
    const buf = Buffer.isBuffer(chunk) ? Buffer.from(chunk) : Buffer.from(chunk, encoding)
    held.push(buf)
    heldBytes += buf.length
    heldPackets++
    if (typeof encoding === 'function') encoding()
    else if (typeof cb === 'function') cb()
    return true
  }

  const tick = setInterval(() => {
    process.stdout.write(`\r[flush] holding ${heldPackets} packets / ${heldBytes} bytes...   `)
  }, 500)

  await new Promise(r => setTimeout(r, HOLD_SECONDS * 1000))
  clearInterval(tick)
  clearInterval(mover)
  bot.clearControlStates()

  posAfterHold = { ...bot.entity.position }
  flushedAt = Date.now()
  phase = 'settling'
  console.log(`\n[flush] releasing ${heldPackets} packets (${heldBytes} bytes) in one burst`)

  socket.write = realWrite
  for (const chunk of held) realWrite(chunk)
  held = []

  console.log(`[flush] flushed — watching ${secs(SETTLE_MS)}s for a kick or correction`)
  setTimeout(() => report('survived', `still connected ${secs(Date.now() - flushedAt)}s after flush`), SETTLE_MS)
})

process.on('uncaughtException', (err) => {
  if (finished) return
  console.error(`\n[flush] failed during ${phase}: ${err.message}`)
  process.exit(1)
})
