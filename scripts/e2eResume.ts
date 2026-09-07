#!/usr/bin/env tsx
// End-to-end resume test: the real client stack, the real proxy, a real server.
//
// Everything below the bot is what the browser runs — net-browserify's Socket
// with resumableSocket's patch on it, a WebSocket to MWCProxy, and MWCProxy's
// TCP connection to Minecraft. What is missing relative to a browser is only the
// browser: pointer lock, the HUD, the inventory. Those still need a human.
//
// The drop is a real one. A TCP cutter sits between the client and the proxy and
// resets live connections, so the client sees a link that died rather than a
// socket someone asked to close politely — which is the distinction the whole
// design turns on.
//
// Two bots, because a claim about a byte stream needs a witness at the far end:
//
//   probe     joins through the proxy; this is the client under test
//   observer  joins Minecraft directly; sees what the server actually received
//
// During each outage the probe speaks and the observer speaks. After the resume,
// the observer must have heard the probe (outbound queued and delivered) and the
// probe must have heard the observer (inbound buffered and replayed). Neither is
// provable from one side alone.
//
// Usage:
//   npx tsx scripts/e2eResume.ts
//   OUTAGES=5 OUTAGE_MS=5000 npx tsx scripts/e2eResume.ts

import net from 'net'
import { setTimeout as sleep } from 'timers/promises'

const MC_HOST = process.env.MC_HOST ?? '127.0.0.1'
const MC_PORT = Number(process.env.MC_PORT ?? 25565)
const PROXY_PORT = Number(process.env.PROXY_PORT ?? 8081)
const CUT_PORT = Number(process.env.CUT_PORT ?? 8082)
const VERSION = process.env.MC_VERSION ?? '1.21.4'
// A fresh name each run, so the probe spawns at world spawn rather than wherever
// the previous run's probe happened to log out — which after a few runs is
// wherever it got wedged.
const PROBE = process.env.MC_USERNAME ?? `Probe${process.pid % 100000}`
const WITNESS = process.env.MC_WITNESS ?? 'ResumeWitness'
// Where to stand before testing movement. Requires the probe to be opped.
const START_AT = process.env.START_AT ?? ''
// Matches the web client's heartbeat (src/mineflayer/plugins/ping.ts). It is not
// optional: acknowledgements are the only thing that frees the proxy's buffer,
// so a client that never sends one stalls as soon as the server has produced
// StreamBufferBytes of output.
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS ?? 5000)
/**
 * What the world does behind the player's back while the link is down.
 *
 * The queued movement measured by the plain run is movement the server would
 * have accepted anyway. These are the cases where it could not have happened:
 * the ground the player walked over is now a wall, or the player was dead the
 * whole time. What the server does with a burst of impossible movement is the
 * question — accept, correct, or disconnect.
 *
 *   none      the plain run
 *   wall      a barrier is built across the path mid-outage
 *   death     the player is killed mid-outage
 *   teleport  the player is moved somewhere else mid-outage
 *   all       one outage of each, in that order
 */
const CONFLICT = process.env.CONFLICT ?? 'none'
/**
 * How long the player walks during an outage, as opposed to how long the outage
 * lasts.
 *
 * They are separate because the flat area is finite: walk for a whole long
 * outage and the probe leaves it, falls, and the fall shows up as a correction
 * that has nothing to do with what the server made of the movement burst.
 */
const WALK_MS = Number(process.env.WALK_MS ?? 2000)
const OUTAGES = Number(process.env.OUTAGES ?? 3)
const OUTAGE_MS = Number(process.env.OUTAGE_MS ?? 4000)
// A last, longer outage aimed near the proxy's resume deadline. Zero skips it.
const LONG_OUTAGE_MS = Number(process.env.LONG_OUTAGE_MS ?? 0)

const log = (...args: any[]) => console.log(...args)

// --- shims: net-browserify is browser code -----------------------------------

const g = globalThis as any
const u = net as any // placeholder so the import is used before the real work
void u

const installShims = async () => {
  const util = (await import('util')) as any
  const uu = util.default ?? util
  uu.isNumber ??= (v: any) => typeof v === 'number'
  uu.isString ??= (v: any) => typeof v === 'string'
  uu.isFunction ??= (v: any) => typeof v === 'function'
  uu.isUndefined ??= (v: any) => v === undefined
  uu.isObject ??= (v: any) => v !== null && typeof v === 'object'
  uu.isBuffer ??= (v: any) => Buffer.isBuffer(v)

  const timers = (await import('timers')) as any
  const t = timers.default ?? timers
  t.unenroll ??= () => {}

  g.window ??= {
    location: { protocol: 'http:', hostname: '127.0.0.1', port: String(CUT_PORT) },
    addEventListener () {},
    removeEventListener () {},
  }
}

// --- the cutter --------------------------------------------------------------

/**
 * A TCP relay that can sever what is passing through it.
 *
 * Reset rather than close: a FIN is a polite hangup and the client is entitled
 * to treat it as the peer leaving. A venue Wi-Fi drop is not polite.
 */
class Cutter {
  server: net.Server
  pairs = new Set<[net.Socket, net.Socket]>()
  cuts = 0
  /** Whether the route exists at all. While false, dialling it fails. */
  private routed = true

  constructor (private readonly toPort: number, private readonly listenPort: number) {
    this.server = net.createServer(client => {
      // A severed link is not one that reconnects on the next attempt. Cutting
      // live connections while still accepting new ones makes every outage last
      // exactly one reconnect backoff, whatever the test asked for — which is
      // how an early version of this harness reported twenty-second outages
      // that were really two hundred and fifty milliseconds.
      if (!this.routed) {
        client.destroy()
        return
      }
      const upstream = net.connect(this.toPort, '127.0.0.1')
      const pair: [net.Socket, net.Socket] = [client, upstream]
      this.pairs.add(pair)

      client.pipe(upstream)
      upstream.pipe(client)

      const drop = () => {
        this.pairs.delete(pair)
        client.destroy()
        upstream.destroy()
      }
      client.on('error', drop)
      upstream.on('error', drop)
      client.on('close', drop)
      upstream.on('close', drop)
    })
  }

  async listen () {
    await new Promise<void>(resolve => {
      this.server.listen(this.listenPort, '127.0.0.1', () => resolve())
    })
  }

  /** Take the route away: cut what is live and refuse what tries to come back. */
  down () {
    this.routed = false
    return this.cut()
  }

  /** Put it back. */
  up () {
    this.routed = true
  }

  cut () {
    const live = [...this.pairs]
    for (const pair of live) {
      this.pairs.delete(pair)
      for (const s of pair) {
        if (typeof (s as any).resetAndDestroy === 'function') (s as any).resetAndDestroy()
        else s.destroy()
      }
    }
    this.cuts++
    return live.length
  }

  close () {
    this.cut()
    this.server.close()
  }
}

// --- bots --------------------------------------------------------------------

const spawned = async (bot: any, who: string, timeoutMs = 60_000) =>
  new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${who} never spawned`)), timeoutMs)
    bot.once('spawn', () => { clearTimeout(timer); resolve() })
    bot.once('error', (e: any) => { clearTimeout(timer); reject(new Error(`${who}: ${e?.message ?? e}`)) })
    bot.once('end', (r: any) => { clearTimeout(timer); reject(new Error(`${who} ended before spawn: ${r}`)) })
  })

const main = async () => {
  await installShims()

  const cutter = new Cutter(PROXY_PORT, CUT_PORT)
  await cutter.listen()
  log(`cutter    127.0.0.1:${CUT_PORT} -> 127.0.0.1:${PROXY_PORT}`)

  const browserNet = (await import('net-browserify/browser.js') as any).default ??
    await import('net-browserify/browser.js')

  // resumableSocket does `import net from 'net'`, which the app's bundler aliases
  // to this module. Nothing aliases it here, so hand it over directly: the patch
  // then lands on the same prototype the browser patches. node's own `net` is
  // unaffected — its internals reference the class, not the export — so the
  // observer bot below still connects normally.
  const nodeNet = net as any
  const realSocket = nodeNet.Socket
  nodeNet.Socket = browserNet.Socket
  const { patchResumableSocket, getResumeState, resumeEvents } = await import('../src/mineflayer/resumableSocket')
  patchResumableSocket()
  nodeNet.Socket = realSocket

  browserNet.setProxy({ hostname: 'http://127.0.0.1', port: String(CUT_PORT) })

  const mineflayer = (await import('mineflayer')).default

  // The witness, straight to Minecraft.
  const observer = mineflayer.createBot({
    host: MC_HOST, port: MC_PORT, username: WITNESS, auth: 'offline', version: VERSION,
  })
  await spawned(observer, 'observer')
  log('observer  joined directly')

  const heardByObserver: string[] = []
  observer.on('chat', (_username: string, message: string) => { heardByObserver.push(message) })

  // The client under test, through the proxy.
  let probeSocket: any
  const pongs: string[] = []
  const probe = mineflayer.createBot({
    host: MC_HOST, port: MC_PORT, username: PROBE, auth: 'offline', version: VERSION,
    // What the web client sets (src/index.ts:606). Not a detail: minecraft-
    // protocol's own watchdog ends the session when no keep-alive has arrived
    // for this long, and during an outage none can — they are queued in the
    // proxy. At the 30 s default a twenty-second outage kills the session from
    // the client side before the resume ever completes, and the failure looks
    // like a server kick when it is nothing of the kind.
    checkTimeoutInterval: 240 * 1000,
    closeTimeout: 240 * 1000,
    connect: (client: any) => {
      const socket = new browserNet.Socket()
      probeSocket = socket
      socket.connect({ port: MC_PORT, host: MC_HOST })
      client.setSocket(socket)
      client.emit('connect')

      socket.on('pong', (payload: string) => { pongs.push(String(payload)) })

      // Report what we have received, the way the real client's ping plugin
      // does. Read _ws afresh each time: it is replaced by every resume.
      setInterval(() => {
        const ws = socket._ws
        if (ws?.readyState !== 1) return
        try {
          ws.send(`ping:0:${socket.bytesRead ?? 0}`)
        } catch { /* the socket died between the check and the send */ }
      }, HEARTBEAT_MS).unref()
    },
  } as any)

  const probeEnded: string[] = []
  probe.on('end', (reason: any) => probeEnded.push(String(reason)))
  probe.on('kicked', (reason: any) => probeEnded.push(`kicked: ${JSON.stringify(reason)}`))
  /**
   * Where the server thinks the probe is, as another client sees it.
   *
   * Taken from the witness rather than the probe, because the probe's own
   * position is whatever its local physics decided — including during an outage,
   * when nothing has confirmed it. Only another client sees what the server
   * accepted.
   *
   * Undefined when the witness is too far away to be sent the probe's entity at
   * all, which is not the same as the probe standing still and must never be
   * read as it.
   */
  const probeAt = () => observer.players[PROBE]?.entity?.position?.clone?.()
  const witnessSeesProbe = () => probeAt() !== undefined

  const heardByProbe: string[] = []
  probe.on('chat', (_username: string, message: string) => { heardByProbe.push(message) })

  const deaths: string[] = []
  probe.on('death', () => { deaths.push(new Date().toISOString()) })
  // Server-forced repositioning. This is what a rejected movement burst looks
  // like from the client's side: the server says where the player actually is.
  const forcedMoves: number[] = []
  probe._client.on('position', (packet: any) => {
    const at = probe.entity?.position
    if (at) forcedMoves.push(Math.hypot(packet.x - at.x, packet.y - at.y, packet.z - at.z))
  })

  await spawned(probe, 'probe')
  log(`probe     ${PROBE} joined through the proxy at ${probe.entity.position}`)
  try {
    await probe.waitForChunksToLoad()
  } catch { /* physics can start without every chunk */ }

  // Put the two of them together. Entities are only sent to clients within view
  // distance, so a witness left at world spawn simply never hears about a probe
  // standing 176 blocks away — and its silence would read as "the player did not
  // move". Needs the probe opped; without that the server refuses and the run
  // reports what the witness can and cannot see.
  if (START_AT) {
    log(`probe     teleporting to ${START_AT}`)
    probe.chat(`/tp ${START_AT}`)
    await sleep(1500)
  }
  probe.chat(`/tp ${WITNESS} ${PROBE}`)
  // The witness needs to be able to change the world while the probe is away.
  // The probe is opped, and an opped player can op another.
  probe.chat(`/op ${WITNESS}`)
  await sleep(1500)

  // A teleport streams a whole new region, which with compression on is a lot of
  // bytes. Let that finish before anything is measured, or the first
  // measurement is really measuring the chunk burst.
  try {
    await probe.waitForChunksToLoad()
  } catch { /* not every chunk has to arrive */ }
  await sleep(2000)

  // Let it land before measuring anything: a bot still falling is moving for
  // reasons that have nothing to do with what is being tested.
  for (let i = 0; i < 50 && !probe.entity.onGround; i++) await sleep(100)
  log(`probe     settled at ${probe.entity.position} (onGround ${probe.entity.onGround})`)
  log(`witness   ${witnessSeesProbe() ? 'can see' : 'CANNOT see'} the probe`)

  resumeEvents.addEventListener('lost', () => log('  [client] transport lost'))
  resumeEvents.addEventListener('resumed', (e: any) => log(`  [client] resumed (replayed ${e.detail?.replayed ?? 0} bytes)`))
  let lostAt: number | undefined
  let gaveUpAfterMs: number | undefined
  resumeEvents.addEventListener('lost', () => { lostAt ??= Date.now() })
  resumeEvents.addEventListener('unresumable', (e: any) => {
    gaveUpAfterMs = lostAt === undefined ? undefined : Date.now() - lostAt
    log(`  [client] UNRESUMABLE after ${gaveUpAfterMs}ms: ${e.detail?.reason}`)
  })

  // Sanity, both directions: an assertion about an outage means nothing unless
  // the same assertion holds when nothing is wrong.
  probe.chat('probe-online')
  observer.chat('witness-online')
  await sleep(4000)
  const outboundBaseline = heardByObserver.includes('probe-online')
  const inboundBaseline = heardByProbe.includes('witness-online')
  log(`baseline  observer heard the probe: ${outboundBaseline}`)
  log(`baseline  probe heard the witness:  ${inboundBaseline}`)
  if (!outboundBaseline || !inboundBaseline) {
    log(`  probe heard: ${JSON.stringify(heardByProbe)}`)
    log(`  witness heard: ${JSON.stringify(heardByObserver)}`)
    throw new Error('the two bots cannot hear each other with nothing broken; nothing below would mean anything')
  }

  /**
   * Which way there is ground to walk on.
   *
   * Facing a compass point and hoping is how the first attempt at the wall test
   * placed a barrier in mid-air off the edge of the plain: the probe walked over
   * the edge before it ever reached the wall, and the run reported no conflict
   * because there had not been one.
   */
  const pickOpenDirection = () => {
    const at = probe.entity.position
    const dirs = [[1, 0], [-1, 0], [0, 1], [0, -1]]
    let best = { dx: 1, dz: 0, run: 0 }

    for (const [dx, dz] of dirs) {
      let run = 0
      for (let d = 1; d <= 30; d++) {
        const under = probe.blockAt(at.offset(dx * d, -1, dz * d))
        const head = probe.blockAt(at.offset(dx * d, 0, dz * d))
        if (!under || under.boundingBox !== 'block') break
        if (head && head.boundingBox === 'block') break
        run = d
      }
      if (run > best.run) best = { dx, dz, run }
    }
    return best
  }

  const heading = pickOpenDirection()
  log(`terrain   ${heading.run} blocks of open ground toward (${heading.dx}, ${heading.dz})`)
  const faceForward = async () => {
    const at = probe.entity.position
    await probe.lookAt(at.offset(heading.dx * 20, 0, heading.dz * 20))
  }

  // Does the bot move at all? If terrain has it wedged, a displacement of zero
  // after an outage would mean nothing.
  await faceForward()
  probe.setControlState('forward', true)
  const walkFrom = probeAt()
  await sleep(2000)
  probe.setControlState('forward', false)
  await sleep(500)
  const walkTo = probeAt()
  const walked = walkFrom && walkTo ? walkFrom.distanceTo(walkTo) : 0
  log(`baseline  the witness sees the probe walk ${walked.toFixed(2)} blocks in 2s` +
    ` (feet: ${probe.blockAt(probe.entity.position)?.name}, ahead: ${probe.blockAt(probe.entity.position.offset(0, 0, 1))?.name})`)
  if (!witnessSeesProbe()) {
    throw new Error('the witness cannot see the probe, so nothing it reports about movement means anything')
  }
  if (walked < 0.5) {
    throw new Error('the probe does not move with nothing broken; movement across an outage would not be tested')
  }

  const results: Array<{
    n: number, outbound: boolean, inbound: boolean, resumes: number, ended: boolean, moved: number,
    correction: number, conflict: string, died: number, forced: number, health: number
  }> = []

  /**
   * Make the queued movement impossible, from the witness, while the probe is
   * disconnected.
   *
   * Note what the probe does *not* see: the block update, the death, the
   * teleport are all sitting in the proxy's buffer for the whole outage. Its
   * local physics walks on in ignorance and queues movement for ground that is
   * now a wall. Both the world change and the movement land at once on resume,
   * which is exactly the collision this is testing.
   */
  const applyConflict = async (kind: string, from: any, now: any) => {
    switch (kind) {
      case 'wall': {
        // Across the path rather than at a compass point: the direction comes
        // from where the probe has actually walked, so this does not depend on
        // which way any yaw convention points.
        const dx = now.x - from.x
        const dz = now.z - from.z
        const len = Math.hypot(dx, dz) || 1
        const ahead = 4
        const cx = Math.round(now.x + (dx / len) * ahead)
        const cz = Math.round(now.z + (dz / len) * ahead)
        const y = Math.round(now.y)
        // Perpendicular to the direction of travel, so it spans the path.
        const px = Math.round(-(dz / len) * 4)
        const pz = Math.round((dx / len) * 4)
        observer.chat(`/fill ${cx - px} ${y} ${cz - pz} ${cx + px} ${y + 3} ${cz + pz} stone`)
        log(`  [world] wall across the path at (${cx}, ${y}, ${cz}), ${ahead} blocks ahead`)
        break
      }
      case 'death': {
        observer.chat(`/kill ${PROBE}`)
        log('  [world] the probe was killed')
        break
      }
      case 'teleport': {
        const x = Math.round(from.x) + 40
        observer.chat(`/tp ${PROBE} ${x} ${Math.round(from.y)} ${Math.round(from.z)}`)
        log(`  [world] the probe was moved to x=${x}`)
        break
      }
      default:
        break
    }
    await sleep(500)
  }

  const conflicts = CONFLICT === 'all' ? ['wall', 'death', 'teleport'] : [CONFLICT]
  const total = CONFLICT === 'none'
    ? OUTAGES + (LONG_OUTAGE_MS > 0 ? 1 : 0)
    : conflicts.length
  for (let n = 1; n <= total; n++) {
    const long = CONFLICT === 'none' && n > OUTAGES
    const holdMs = long ? LONG_OUTAGE_MS : OUTAGE_MS
    const conflict = CONFLICT === 'none' ? 'none' : conflicts[n - 1]
    const outbound = `probe-during-outage-${n}`
    const inbound = `witness-during-outage-${n}`
    const before = getResumeState(probeSocket)?.resumes ?? 0
    const deathsBefore = deaths.length
    const forcedBefore = forcedMoves.length

    // Back to the same spot each time. Five seconds of walking covers enough
    // ground to leave the flat area, and an outage that starts with the probe
    // wedged in terrain measures nothing.
    if (START_AT) {
      probe.chat(`/tp ${START_AT}`)
      await sleep(2000)
      for (let i = 0; i < 30 && !probe.entity.onGround; i++) await sleep(100)
    }
    const from = probeAt()

    const severed = cutter.down()
    log(`\noutage ${n}  severed ${severed} connection(s), route down for ${holdMs}ms`)

    // The player keeps playing. This is the requirement: not a fast rejoin, but
    // a session that never stopped.
    await faceForward()
    probe.setControlState('forward', true)
    probe.chat(outbound)
    observer.chat(inbound)

    const walkMs = Math.min(WALK_MS, holdMs)
    if (conflict !== 'none') {
      await sleep(Math.min(800, walkMs / 2))
      await applyConflict(conflict, from ?? probe.entity.position, probe.entity.position)
      await sleep(Math.max(0, walkMs - 800))
    } else {
      await sleep(walkMs)
    }

    probe.setControlState('forward', false)
    await sleep(200) // let the local physics settle before reading the claim

    // Where the client believes it walked to. Nothing has confirmed it: these
    // are the packets sitting in the queue, and this is the position the server
    // will either accept or correct.
    const claimed = probe.entity.position.clone()
    log(`  walked ${walkMs}ms while disconnected, to ${claimed}`)

    // The rest of the outage, standing still.
    await sleep(Math.max(0, holdMs - walkMs))

    // The route comes back. The client is somewhere in its backoff, so the
    // reconnect lands up to one backoff interval later — which is what happens
    // at a venue too.
    cutter.up()
    log(`  route restored after ${holdMs}ms; waiting for the client to notice`)

    // Wait for the shim to get back.
    const deadline = Date.now() + 30_000
    while ((getResumeState(probeSocket)?.resumes ?? 0) === before && Date.now() < deadline) {
      await sleep(100)
    }
    const resumes = getResumeState(probeSocket)?.resumes ?? 0
    await sleep(2500) // let the replay land and the server answer

    const to = probeAt()
    results.push({
      n,
      outbound: heardByObserver.includes(outbound),
      inbound: heardByProbe.includes(inbound),
      resumes,
      ended: probeEnded.length > 0,
      moved: from && to ? from.distanceTo(to) : 0,
      // How far the server put the player from where the client thought it had
      // walked. A queued burst the server rejects comes back as a correction,
      // and this is what that correction would measure.
      correction: to ? claimed.distanceTo(to) : Number.NaN,
      conflict,
      died: deaths.length - deathsBefore,
      // Server-forced repositions during and after the burst: the shape a
      // rejection takes when it is not a disconnection.
      forced: forcedMoves.length - forcedBefore,
      health: probe.health,
    })
    const r = results.at(-1)!
    log(`outage ${n}  conflict=${r.conflict} resumes=${r.resumes} outbound=${r.outbound} inbound=${r.inbound} ` +
      `moved=${r.moved.toFixed(2)} correction=${r.correction.toFixed(2)} forced=${r.forced} ` +
      `died=${r.died} health=${r.health} ended=${r.ended}`)
    if (r.ended) break
  }

  log('\n--- result ---')
  // A movement queued through an outage has to actually take effect: delivered
  // but rejected is not the same as delivered.
  // In a conflict run the movement is supposed to be impossible, so distance
  // proves nothing and only survival does: the session must not end, and the
  // stream must still be carrying traffic in both directions afterwards.
  const MOVED_ENOUGH = 1 // blocks; anything less is standing still
  const ok = CONFLICT === 'none'
    ? results.every(r => r.outbound && r.inbound && !r.ended && r.moved >= MOVED_ENOUGH) && probeEnded.length === 0
    : results.every(r => !r.ended) && probeEnded.length === 0
  for (const r of results) {
    log(`outage ${r.n} [${r.conflict}]: outbound ${r.outbound ? 'delivered' : 'LOST'}, ` +
      `inbound ${r.inbound ? 'replayed' : 'LOST'}, the server moved the player ${r.moved.toFixed(2)} blocks, ` +
      `corrected it by ${r.correction.toFixed(2)} over ${r.forced} forced move(s), ` +
      `${r.died} death(s), health ${r.health}, session ${r.ended ? 'ENDED' : 'alive'}`)
  }
  log(`probe position after ${OUTAGES} outages: ${probe.entity?.position}`)
  log(`probe heard:   ${JSON.stringify(heardByProbe)}`)
  log(`witness heard: ${JSON.stringify(heardByObserver)}`)
  const st = getResumeState(probeSocket)
  log(`client window: acked=${st?.out.acked} cursor=${st?.out.position} produced=${st?.out.produced} held=${st?.out.length}`)
  log(`pongs seen by the client: ${pongs.length}${pongs.length ? ` (last ${pongs.at(-1)})` : ''}`)
  log(`session end events: ${probeEnded.length === 0 ? 'none' : probeEnded.join(' | ')}`)
  log(`unresumable after: ${gaveUpAfterMs === undefined ? 'n/a' : `${gaveUpAfterMs}ms`}`)
  log(ok ? '\nPASS — the session survived every outage in both directions' : '\nFAIL')

  probe.quit()
  observer.quit()
  cutter.close()
  await sleep(500)
  process.exit(ok ? 0 : 1)
}

main().catch(err => {
  console.error('\nharness error:', err?.message ?? err)
  process.exit(2)
})
