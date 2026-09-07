// Tests for the client half of transparent resume.
//
// This is browser code, but very little of it is about the browser: it is
// transport plumbing, and the claim that matters — that losing a WebSocket does
// not take the Duplex, or the player's actions, with it — is testable in node
// against a real WebSocket server. What is genuinely browser-only (pointer lock,
// the HUD, whether an open inventory survives) still needs the live setup.

import { createRequire } from 'module'
import net from 'net'
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { getResumeState, patchResumableSocket, type ResumeState } from './resumableSocket'

// The app aliases `net` to net-browserify's browser build (rsbuildSharedConfig),
// so that is the module under test — node's builtin `net` shares the name and
// nothing else. Two shims go in first, both narrow: net-browserify reads
// window.location as it loads, and it predates node dropping the util type
// predicates the browser bundler still substitutes.
//
// Inlined rather than factored out because vi.mock is hoisted above every import
// in this file, and the factory must be self-contained.
vi.mock('net', async () => {
  const util = (await import('util')) as any
  const u = util.default ?? util
  u.isNumber ??= (v: any) => typeof v === 'number'
  u.isString ??= (v: any) => typeof v === 'string'
  u.isFunction ??= (v: any) => typeof v === 'function'
  u.isUndefined ??= (v: any) => v === undefined
  u.isObject ??= (v: any) => v !== null && typeof v === 'object'
  u.isBuffer ??= (v: any) => Buffer.isBuffer(v)

  // Removed from node, still present in the timers-browserify shim the bundler
  // substitutes; net-browserify calls it on destroy.
  const timers = (await import('timers')) as any
  const t = timers.default ?? timers
  t.unenroll ??= () => {}

  ;(globalThis as any).window ??= {
    location: { protocol: 'http:', hostname: '127.0.0.1', port: '0' },
    addEventListener () {},
    removeEventListener () {},
  }

  return import('net-browserify/browser.js')
})

// `ws` ships no types of its own and the app never imports it; requiring it
// keeps TypeScript from resolving a stale declaration for a test-only server.
const { WebSocketServer } = createRequire(import.meta.url)('ws')

// net-browserify's surface is nothing like node's `net`, whose types TypeScript
// resolves for this specifier no matter what the bundler aliases it to.
const netLib = net as any

// --- a fake proxy ----------------------------------------------------------

class FakeProxy {
  server: any
  port = 0
  sockets: any[] = []
  /** Every binary frame received, in arrival order across all connections. */
  received: Buffer[] = []
  /** The URL of each connection, so the resume handshake can be asserted. */
  urls: string[] = []
  /** What the proxy claims to have accepted, reported in its pongs. */
  reportReceived = 0
  /** Every control frame received, in order. */
  control: string[] = []
  /** Replayed bytes still to discard, per the client's `sent:` frame. */
  skip = 0
  /** The offset each connection opened with, in order. */
  resumedFrom: number[] = []
  /** Control frames, kept per connection so their order can be asserted. */
  controlByConn: string[][] = []

  constructor () {
    this.server = new WebSocketServer({ port: 0 })
    this.server.on('connection', (ws: any, req: any) => {
      this.urls.push(req.url ?? '')
      this.sockets.push(ws)
      const control: string[] = []
      this.controlByConn.push(control)
      ws.on('message', (data: Buffer, isBinary: boolean) => {
        if (isBinary) {
          let buf = Buffer.from(data)
          if (this.skip > 0) {
            const drop = Math.min(this.skip, buf.length)
            this.skip -= drop
            buf = buf.subarray(drop)
          }
          if (buf.length > 0) this.received.push(buf)
          return
        }
        const msg = data.toString()
        this.control.push(msg)
        control.push(msg)
        if (msg.startsWith('resume:')) {
          this.resumedFrom.push(Number(msg.slice('resume:'.length)))
          return
        }
        if (msg.startsWith('ping:')) {
          const [seq] = msg.slice('ping:'.length).split(':')
          ws.send(`pong:${seq}:${this.reportReceived}`)
          return
        }
        if (msg.startsWith('sent:')) {
          // What the real proxy does with it: discard the overlap between where
          // the client is replaying from and what we already accepted.
          this.skip = Math.max(0, this.bytesReceived() - Number(msg.slice('sent:'.length)))
        }
      })
    })
  }

  async ready () {
    await new Promise<void>(resolve => {
      this.server.once('listening', () => resolve())
    })
    this.port = this.server.address().port
  }

  get current () { return this.sockets.at(-1) }

  /** Drop the live socket with no close handshake — what a bad network does. */
  kill () {
    this.current._socket.destroy()
  }

  bytesReceived () { return Buffer.concat(this.received).length }
  allReceived () { return Buffer.concat(this.received).toString() }

  close () {
    for (const ws of this.sockets) ws.terminate()
    this.server.close()
  }
}

const wait = async (ms: number) => new Promise(resolve => { setTimeout(resolve, ms) })

const waitFor = async (what: string, cond: () => boolean, timeout = 3000) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (cond()) return
    // eslint-disable-next-line no-await-in-loop -- polling is the point
    await wait(5)
  }
  throw new Error(`timed out waiting for ${what}`)
}

// --- tests -----------------------------------------------------------------

describe('resumableSocket', () => {
  let proxy: FakeProxy
  let sockets: any[] = []

  beforeAll(() => {
    patchResumableSocket()
  })

  afterEach(() => {
    for (const s of sockets) {
      getResumeState(s)!.closing = true // stop any reconnect loop still running
      s._ws?.close()
    }
    sockets = []
    proxy?.close()
  })

  /**
   * A Socket wired to the fake proxy.
   *
   * This goes straight to _connectWebSocket, the way a resume does, rather than
   * through connect()'s HTTP round trip to /connect — which is the part of the
   * flow that opens a *new* TCP connection to Minecraft, and so is exactly what
   * a resume must not do.
   */
  const connect = async (wsTimeout = 2000) => {
    proxy = new FakeProxy()
    await proxy.ready()
    netLib.setProxy({ hostname: 'http://127.0.0.1', port: String(proxy.port) })

    const socket = new netLib.Socket({ wsTimeout })
    sockets.push(socket)
    socket._connecting = true
    socket.writable = true
    socket._connectWebSocket('test-token')

    await waitFor('the socket to connect', () => socket.readyState === 'open')
    return socket
  }

  const state = (socket: any): ResumeState => getResumeState(socket)!

  it('carries writes while the transport is up', async () => {
    const socket = await connect()
    socket.write(Buffer.from('hello'))
    await waitFor('delivery', () => proxy.bytesReceived() === 5)
    expect(proxy.allReceived()).toBe('hello')
  })

  it('delivers inbound frames into the Duplex', async () => {
    const socket = await connect()
    const chunks: Buffer[] = []
    socket.on('data', (c: Buffer) => chunks.push(c))

    proxy.current.send(Buffer.from('from minecraft'), { binary: true })
    await waitFor('inbound data', () => Buffer.concat(chunks).toString() === 'from minecraft')
    expect(socket.bytesRead).toBe(14)
  })

  it('keeps the Duplex alive when the transport drops', async () => {
    // The whole premise. minecraft-protocol wires the cipher and the splitter
    // onto this object once at connect time; destroying it discards the cipher
    // position, the half-read frame and the world model — everything a resume
    // exists to preserve. net-browserify's own close handler destroyed it.
    const socket = await connect()
    proxy.kill()

    await waitFor('the drop to register', () => socket._ws.readyState !== 1)
    await wait(50)

    expect(socket.destroyed).toBeFalsy()
    expect(socket.readyState).toBe('open')
  })

  it('retains writes made while there is no transport', async () => {
    // net-browserify called ws.send() unconditionally, which on a closing socket
    // discards silently. mineflayer's physics loop keeps writing throughout an
    // outage, so everything the player did during it was thrown away.
    const socket = await connect()
    proxy.kill()
    await waitFor('the drop to register', () => socket._ws.readyState !== 1)

    socket.write(Buffer.from('walked forward during the outage'))
    expect(state(socket).pendingBytes).toBeGreaterThan(0)
  })

  it('reconnects and replays what the proxy has not confirmed', async () => {
    const socket = await connect()

    socket.write(Buffer.from('before'))
    await waitFor('the first delivery', () => proxy.bytesReceived() === 6)

    proxy.kill()
    await waitFor('the drop to register', () => socket._ws.readyState !== 1)
    socket.write(Buffer.from('during'))

    await waitFor('a resume', () => state(socket).resumes === 1, 5000)
    await waitFor('the replay', () => proxy.allReceived().includes('during'), 5000)

    // The client replays everything unconfirmed — including 'before', which the
    // proxy already has — and states where the replay starts so the proxy can
    // drop the overlap. What reaches the far end is each byte exactly once.
    expect(proxy.allReceived()).toBe('beforeduring')
    expect(proxy.control).toContain('sent:0')
  })

  it('opens every connection by stating its position', async () => {
    // The proxy has no safe value to assume in place of this. Resuming from the
    // last acknowledgement would replay everything since — bytes we already hold
    // — and a duplicate corrupts a byte stream exactly as thoroughly as a gap,
    // with nothing on this side to catch it.
    const socket = await connect()
    await waitFor('the opening handshake', () => proxy.resumedFrom.length === 1)
    expect(proxy.resumedFrom[0]).toBe(0) // the first connection states zero

    proxy.current.send(Buffer.from('0123456789'), { binary: true })
    await waitFor('inbound data', () => socket.bytesRead === 10)

    proxy.kill()
    await waitFor('a reconnect', () => proxy.resumedFrom.length === 2, 5000)

    expect(proxy.resumedFrom[1]).toBe(10)
  })

  it('states its position before anything else on the wire', async () => {
    // The proxy replays the instant it has the offset, so a frame that arrived
    // after data would be describing a stream that had already moved.
    const socket = await connect()
    socket.write(Buffer.from('hello'))
    await waitFor('delivery', () => proxy.bytesReceived() === 5)

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)
    await waitFor('the replay', () => proxy.control.length >= 3, 5000)

    // On the resumed socket: our position in their stream, then where our own
    // replay begins, and only then bytes.
    expect(proxy.controlByConn[1]).toEqual(['resume:0', 'sent:0'])
  })

  it('continues the same byte stream across a resume', async () => {
    const socket = await connect()
    const chunks: Buffer[] = []
    socket.on('data', (c: Buffer) => chunks.push(c))

    proxy.current.send(Buffer.from('before'), { binary: true })
    await waitFor('the first chunk', () => Buffer.concat(chunks).toString() === 'before')

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)

    proxy.current.send(Buffer.from('after'), { binary: true })
    await waitFor('the second chunk', () => Buffer.concat(chunks).toString() === 'beforeafter', 5000)
    expect(socket.bytesRead).toBe(11)
  })

  it('frees retained output only when the proxy confirms it', async () => {
    // A send is not a delivery: bytes in a dying socket's buffer are lost with
    // no error anywhere, so only the peer's own count may free anything.
    const socket = await connect()

    socket.write(Buffer.from('0123456789'))
    await waitFor('delivery', () => proxy.bytesReceived() === 10)
    expect(state(socket).pendingBytes).toBe(10) // sent, but unconfirmed

    proxy.reportReceived = 6
    socket._ws.send('ping:1:0') // provoke a pong carrying the proxy's count
    await waitFor('the acknowledgement', () => state(socket).proxyRx === 6)

    expect(state(socket).pendingBytes).toBe(4)
  })

  it('replays only the unconfirmed remainder after an acknowledgement', async () => {
    const socket = await connect()

    socket.write(Buffer.from('0123456789'))
    await waitFor('delivery', () => proxy.bytesReceived() === 10)

    proxy.reportReceived = 6
    socket._ws.send('ping:1:0')
    await waitFor('the acknowledgement', () => state(socket).proxyRx === 6)

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)
    await waitFor('the resume handshake', () => proxy.control.includes('sent:6'), 5000)
    await wait(300)

    // The client replays from 6 — the last offset the proxy confirmed — and the
    // proxy drops the four bytes of that replay it had already accepted.
    expect(proxy.allReceived()).toBe('0123456789')
  })

  it('survives longer than the connect timeout after a resume', async () => {
    // net-browserify arms a connect timeout every time a WebSocket is wired up,
    // and clears it on 'open'. A resumed socket is already open, so that event
    // never comes: the timeout fired a few seconds later and closed a perfectly
    // healthy transport, for as long as the outages kept coming.
    const socket = await connect(300)

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)

    const resumed = socket._ws
    await wait(600) // past the timeout the old code would have armed

    expect(socket._ws).toBe(resumed)
    expect(socket._ws.readyState).toBe(1)
    expect(socket.destroyed).toBeFalsy()
  })

  it('does not reconnect after the player quits', async () => {
    // Quitting must not look like a drop, or leaving the game would silently
    // dial back in.
    const socket = await connect()
    socket.end()

    await wait(700)
    expect(proxy.sockets.length).toBe(1)
    expect(state(socket).resumes).toBe(0)
  })

  it('does not reconnect when the proxy closes cleanly', async () => {
    // A close handshake means the far end is finished — the Minecraft server
    // disconnected us, or the proxy is shutting down. There is no session left
    // to resume onto, and retrying would just fail until the window ran out.
    const socket = await connect()
    const closes: any[] = []
    socket.on('close', () => closes.push(1))

    proxy.current.close(1000, 'done')

    await waitFor('the socket to close', () => socket.readyState !== 'open')
    await wait(600)

    expect(proxy.sockets.length).toBe(1)
    expect(socket.destroyed).toBe(true)
    // minecraft-protocol ends the client on 'close'. net-browserify's destroy
    // emits nothing, so without this the session would just go quiet.
    expect(closes.length).toBe(1)
  })

  it('does not emit an error on the Duplex when an established transport fails', async () => {
    // minecraft-protocol ends the client on a socket error. An abnormal close
    // fires 'error' before 'close', so passing it through would tear down the
    // session on exactly the drops the resume is meant to cover.
    const socket = await connect()
    const errors: any[] = []
    socket.on('error', (e: any) => errors.push(e))

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)

    expect(errors).toEqual([])
  })

  it('reports a failure to connect in the first place', async () => {
    // The other side of that: before anything has opened, a failure is a real
    // connect error and the player needs to hear about it.
    const proxyPort = await (async () => {
      const p = new FakeProxy()
      await p.ready()
      const { port } = p
      p.close()
      return port // nothing is listening here now
    })()

    netLib.setProxy({ hostname: 'http://127.0.0.1', port: String(proxyPort) })
    const socket = new netLib.Socket({ wsTimeout: 2000 })
    socket._connecting = true
    socket.writable = true

    const error = await new Promise<any>(resolve => {
      socket.on('error', resolve)
      socket._connectWebSocket('test-token')
    })

    expect(String(error)).toMatch(/WebSocket/)
    expect(state(socket).resumes).toBe(0)
  })

  it('still frees retained output after a resume', async () => {
    // The pong listener is installed once, on the Duplex, and the transport
    // under it is replaced. If a resume left it unheard, the client would retain
    // everything it ever sent and eventually declare itself unresumable.
    const socket = await connect()

    socket.write(Buffer.from('0123456789'))
    await waitFor('delivery', () => proxy.bytesReceived() === 10)

    proxy.kill()
    await waitFor('a resume', () => state(socket).resumes === 1, 5000)

    proxy.reportReceived = 10
    socket._ws.send('ping:1:0')
    await waitFor('the acknowledgement', () => state(socket).proxyRx === 10, 3000)

    expect(state(socket).pendingBytes).toBe(0)
  })

  it('survives repeated drops', async () => {
    const socket = await connect()
    const chunks: Buffer[] = []
    socket.on('data', (c: Buffer) => chunks.push(c))

    for (let i = 0; i < 5; i++) {
      proxy.current.send(Buffer.from(`chunk${i}`), { binary: true })
      // eslint-disable-next-line no-await-in-loop -- the drops are sequential
      await waitFor(`chunk ${i}`, () => Buffer.concat(chunks).toString().endsWith(`chunk${i}`), 5000)
      proxy.kill()
      // eslint-disable-next-line no-await-in-loop -- as above
      await waitFor(`resume ${i + 1}`, () => state(socket).resumes === i + 1, 8000)
    }

    expect(Buffer.concat(chunks).toString()).toBe('chunk0chunk1chunk2chunk3chunk4')
    expect(socket.destroyed).toBeFalsy()
  })
})

describe('resumableSocket — acknowledgement prompts', () => {
  let proxy: FakeProxy
  let sockets: any[] = []

  beforeAll(() => {
    patchResumableSocket()
  })

  afterEach(() => {
    for (const s of sockets) {
      getResumeState(s)!.closing = true
      s._ws?.close()
    }
    sockets = []
    proxy?.close()
  })

  it('answers a prompt with its receive count, without waiting for the heartbeat', async () => {
    // The proxy releases nothing until we confirm it, and only the proxy knows
    // how full its buffer is. So it asks, and the answer has to be prompt: the
    // client's own heartbeat is a timer chosen for latency reporting, with no
    // relation to how fast the server is sending.
    proxy = new FakeProxy()
    await proxy.ready()
    netLib.setProxy({ hostname: 'http://127.0.0.1', port: String(proxy.port) })

    const socket = new netLib.Socket({ wsTimeout: 2000 })
    sockets.push(socket)
    socket._connecting = true
    socket.writable = true
    socket._connectWebSocket('test-token')
    await waitFor('the socket to connect', () => socket.readyState === 'open')

    proxy.current.send(Buffer.from('0123456789'), { binary: true })
    await waitFor('inbound data', () => socket.bytesRead === 10)

    proxy.current.send('ackreq')
    await waitFor('the answer', () => proxy.control.includes('ping:0:10'), 2000)
  })

  it('does not push a prompt into the byte stream', async () => {
    // Unrecognised text frames are handed to handleStringMessage, whose default
    // for the app returns true — which would inject the prompt into the stream
    // as data and corrupt it.
    proxy = new FakeProxy()
    await proxy.ready()
    netLib.setProxy({ hostname: 'http://127.0.0.1', port: String(proxy.port) })

    const socket = new netLib.Socket({ wsTimeout: 2000 })
    sockets.push(socket)
    socket._connecting = true
    socket.writable = true
    socket._connectWebSocket('test-token')
    await waitFor('the socket to connect', () => socket.readyState === 'open')

    const chunks: Buffer[] = []
    socket.on('data', (c: Buffer) => chunks.push(c))

    proxy.current.send('ackreq')
    proxy.current.send(Buffer.from('real data'), { binary: true })
    await waitFor('the data', () => Buffer.concat(chunks).toString() === 'real data', 2000)

    expect(Buffer.concat(chunks).toString()).toBe('real data')
    expect(socket.bytesRead).toBe(9)
  })
})
