#!/usr/bin/env tsx
// What does the resume machinery cost in latency?
//
// The claim it tests: nothing is stalled in the steady state. Bytes go into the
// window and straight out again; the buffer exists to hold what a *departed*
// client has not confirmed, not to pace a connected one.
//
// Method: two bots on the same server at the same moment. The probe goes
// through the proxy — net-browserify, a WebSocket, StreamBuffer, DurableStream —
// and the witness connects to Minecraft directly. Both send `ping_request` and
// wait for `ping_response`, interleaved so that anything drifting over the run
// (server tick load, GC) hits both equally. The difference between them is what
// the proxy path costs.
//
// It measures the whole proxy path, not only this work: one extra network hop,
// the WebSocket framing, and the buffering. On loopback the hop is noise, so
// what is left is essentially the machinery.
//
// Usage:
//   MC_USERNAME=FlushProbe npx tsx scripts/measureProxyLatency.ts
//   ROUNDS=100 npx tsx scripts/measureProxyLatency.ts

import net from 'net'
import { setTimeout as sleep } from 'timers/promises'

const MC_HOST = process.env.MC_HOST ?? '127.0.0.1'
const MC_PORT = Number(process.env.MC_PORT ?? 25565)
const PROXY_PORT = Number(process.env.PROXY_PORT ?? 8081)
const VERSION = process.env.MC_VERSION ?? '1.21.4'
const PROBE = process.env.MC_USERNAME ?? `Lat${process.pid % 100000}`
const WITNESS = process.env.MC_WITNESS ?? 'LatWitness'
const ROUNDS = Number(process.env.ROUNDS ?? 60)

const log = (...a: any[]) => console.log(...a)

const installShims = async () => {
  const util = (await import('util')) as any
  const u = util.default ?? util
  u.isNumber ??= (v: any) => typeof v === 'number'
  u.isString ??= (v: any) => typeof v === 'string'
  u.isFunction ??= (v: any) => typeof v === 'function'
  u.isUndefined ??= (v: any) => v === undefined
  u.isObject ??= (v: any) => v !== null && typeof v === 'object'
  u.isBuffer ??= (v: any) => Buffer.isBuffer(v)
  const timers = (await import('timers')) as any
  ;(timers.default ?? timers).unenroll ??= () => {}
  ;(globalThis as any).window ??= {
    location: { protocol: 'http:', hostname: '127.0.0.1', port: String(PROXY_PORT) },
    addEventListener () {}, removeEventListener () {},
  }
}

const spawned = async (bot: any, who: string) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${who} never spawned`)), 60_000)
    bot.once('spawn', () => { clearTimeout(t); resolve() })
    bot.once('error', (e: any) => { clearTimeout(t); reject(new Error(`${who}: ${e?.message ?? e}`)) })
  })

/** One `ping_request` round trip, in milliseconds. */
const roundTrip = async (bot: any, id: number): Promise<number> =>
  new Promise((resolve, reject) => {
    const sentAt = process.hrtime.bigint()
    const timer = setTimeout(() => {
      bot._client.off('ping_response', onPong)
      reject(new Error('no ping_response within 5s'))
    }, 5000)
    const onPong = (data: any) => {
      if (BigInt(data.id) !== BigInt(id)) return
      clearTimeout(timer)
      bot._client.off('ping_response', onPong)
      resolve(Number(process.hrtime.bigint() - sentAt) / 1e6)
    }
    bot._client.on('ping_response', onPong)
    bot._client.write('ping_request', { id: BigInt(id) })
  })

const stats = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b)
  const at = (p: number) => s[Math.min(s.length - 1, Math.floor(s.length * p))]
  return {
    n: s.length,
    min: s[0],
    median: at(0.5),
    p95: at(0.95),
    max: s.at(-1)!,
    mean: s.reduce((a, b) => a + b, 0) / s.length,
  }
}
const fmt = (s: ReturnType<typeof stats>) =>
  `n=${s.n}  min=${s.min.toFixed(2)}  median=${s.median.toFixed(2)}  mean=${s.mean.toFixed(2)}  p95=${s.p95.toFixed(2)}  max=${s.max.toFixed(2)}  (ms)`

const main = async () => {
  await installShims()
  const browserNet = (await import('net-browserify/browser.js') as any).default ??
    await import('net-browserify/browser.js')

  const nodeNet = net as any
  const realSocket = nodeNet.Socket
  nodeNet.Socket = browserNet.Socket
  const { patchResumableSocket } = await import('../src/mineflayer/resumableSocket')
  patchResumableSocket()
  nodeNet.Socket = realSocket

  browserNet.setProxy({ hostname: 'http://127.0.0.1', port: String(PROXY_PORT) })
  const mineflayer = (await import('mineflayer')).default
  const common = { host: MC_HOST, port: MC_PORT, auth: 'offline' as const, version: VERSION }

  const witness = mineflayer.createBot({ ...common, username: WITNESS })
  await spawned(witness, 'witness')

  const probe = mineflayer.createBot({
    ...common, username: PROBE,
    checkTimeoutInterval: 240_000, closeTimeout: 240_000,
    connect: (client: any) => {
      const socket = new browserNet.Socket()
      socket.connect({ port: MC_PORT, host: MC_HOST })
      client.setSocket(socket)
      client.emit('connect')
      setInterval(() => {
        const ws = socket._ws
        if (ws?.readyState !== 1) return
        try { ws.send(`ping:0:${socket.bytesRead ?? 0}`) } catch {}
      }, 5000).unref()
    },
  } as any)
  await spawned(probe, 'probe')

  log(`\nboth joined; settling before measuring`)
  await sleep(3000)

  const through: number[] = []
  const direct: number[] = []
  for (let i = 0; i < ROUNDS; i++) {
    // Interleaved, so drift over the run lands on both equally.
    through.push(await roundTrip(probe, 1000 + i))
    direct.push(await roundTrip(witness, 5000 + i))
    await sleep(50)
  }

  log(`\nthrough the proxy   ${fmt(stats(through))}`)
  log(`direct to minecraft ${fmt(stats(direct))}`)

  const t = stats(through)
  const d = stats(direct)
  log(`\nproxy path costs    median ${(t.median - d.median).toFixed(2)} ms, p95 ${(t.p95 - d.p95).toFixed(2)} ms`)
  log(`(one extra hop, WebSocket framing and the resume machinery, together)`)

  probe.quit(); witness.quit()
  await sleep(500)
  process.exit(0)
}

main().catch(e => { console.error('\nerror:', e?.message ?? e); process.exit(1) })
