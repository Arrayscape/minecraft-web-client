// Make the client's socket survive losing its WebSocket.
//
// The premise of the whole resume mechanism is that everything above the socket
// stays intact across a drop: minecraft-protocol's cipher position, the
// splitter's partially-received frame, mineflayer's world model. All of that
// hangs off one Duplex object (see minecraft-protocol client.js, where
// `socket.pipe(decipher).pipe(splitter)` and `framer.pipe(cipher).pipe(socket)`
// are wired once at connect time). Replace the Duplex and every bit of it is
// gone; keep the Duplex and swap only the WebSocket underneath, and the protocol
// stack never learns anything happened.
//
// So this patches net-browserify's Socket rather than replacing it: on a lost
// connection it dials a new WebSocket for the same session token and re-attaches
// it to the existing Duplex.
//
// Byte accounting mirrors the proxy's, because neither side can know what the
// other received:
//
//   received  bytes pushed into the Duplex (net-browserify's bytesRead, reported
//             by the heartbeat) so the proxy knows what to replay
//   txTotal   bytes handed to send()
//   proxyRx   what the proxy says it has accepted; everything after it is kept
//             and resent on reconnect
//
// A send is not a delivery. Bytes sitting in a dying socket's buffer are lost
// with no error anywhere, so only the peer's own count may free anything.

import net from 'net'

/** Cap on retained outbound bytes. Past this a resume cannot be honest. */
const MAX_UNCONFIRMED_BYTES = 4 * 1024 * 1024

const RECONNECT_BASE_MS = 250
const RECONNECT_MAX_MS = 5000

export interface ResumeState {
  /** Bytes handed to send(). */
  txTotal: number
  /** What the proxy reports having accepted. */
  proxyRx: number
  /** Sent but unconfirmed, kept for replay. */
  pending: Buffer[]
  pendingBytes: number
  /** Set when the stream can no longer be resumed honestly. */
  broken: boolean
  reconnecting: boolean
  attempts: number
  resumes: number
  /** Set by end()/destroy() so a deliberate close is not treated as a drop. */
  closing: boolean
}

const stateOf = (socket: any): ResumeState => {
  socket._resume ??= {
    txTotal: 0,
    proxyRx: 0,
    pending: [],
    pendingBytes: 0,
    broken: false,
    reconnecting: false,
    attempts: 0,
    resumes: 0,
    closing: false,
  } satisfies ResumeState
  return socket._resume
}

/** Resume state for a socket, for the UI and for tests. */
export const getResumeState = (socket: any): ResumeState | undefined => socket?._resume

/** Fired as the socket loses and regains its transport, for the HUD. */
export const resumeEvents = new EventTarget()

const emit = (name: string, detail?: any) => {
  resumeEvents.dispatchEvent(new CustomEvent(name, { detail }))
}

let patched = false

export const patchResumableSocket = () => {
  if (patched) return
  patched = true

  const { Socket } = net as any
  if (!Socket?.prototype?._write) return // not the browser shim

  const originalWrite = Socket.prototype._write
  const originalConnectWS = Socket.prototype._connectWebSocket
  const originalEnd = Socket.prototype.end
  const originalDestroy = Socket.prototype.destroy

  // Remember how to get back. The proxy keys a session on this token, and
  // reconnecting must NOT go through /connect again — that would open a second
  // TCP connection to the Minecraft server instead of resuming the first.
  Socket.prototype._connectWebSocket = function (token: string, cb: any) {
    const result = originalConnectWS.call(this, token, cb)
    this._resumeUrl = this._ws?.url
    installResumeHandlers(this)
    return result
  }

  Socket.prototype._write = function (chunk: any, encoding: any, callback: any) {
    const state = stateOf(this)
    const buf: Buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding)

    retain(this, state, buf)

    const ws = this._ws
    if (ws && ws.readyState === WebSocket.OPEN) {
      state.txTotal += buf.length
      return originalWrite.call(this, chunk, encoding, callback)
    }

    // No transport. The bytes are retained above, so they go out on reconnect.
    //
    // Reporting success is deliberate: applying backpressure here would stall
    // mineflayer's physics loop, which is the thing keeping the player in
    // control during the outage. Their actions queue instead of being lost.
    callback?.()
    return true
  }

  // A deliberate close must not look like a drop, or quitting would trigger a
  // reconnect attempt.
  Socket.prototype.end = function (...args: any[]) {
    stateOf(this).closing = true
    return originalEnd.apply(this, args)
  }
  Socket.prototype.destroy = function (...args: any[]) {
    stateOf(this).closing = true
    return originalDestroy.apply(this, args)
  }
}

/** Keep a sent chunk until the proxy confirms it. */
const retain = (socket: any, state: ResumeState, buf: Buffer) => {
  if (state.broken) return

  if (state.pendingBytes + buf.length > MAX_UNCONFIRMED_BYTES) {
    // A byte stream cannot skip anything: dropping here would desync the cipher
    // on any later resume. Mark the session unresumable rather than pretend.
    state.broken = true
    console.warn(
      `[resume] unconfirmed output passed ${MAX_UNCONFIRMED_BYTES} bytes; this session can no longer be resumed`
    )
    emit('unresumable', { reason: 'outbound buffer exceeded' })
    return
  }
  state.pending.push(buf)
  state.pendingBytes += buf.length
}

/** Release everything the proxy has confirmed receiving. */
const confirmTo = (state: ResumeState, proxyRx: number) => {
  if (proxyRx <= state.proxyRx) return
  let free = proxyRx - state.proxyRx
  state.proxyRx = proxyRx

  while (free > 0 && state.pending.length > 0) {
    const head = state.pending[0]
    if (head.length <= free) {
      free -= head.length
      state.pendingBytes -= head.length
      state.pending.shift()
    } else {
      // The proxy confirmed part of this chunk; keep the remainder.
      state.pending[0] = head.subarray(free)
      state.pendingBytes -= free
      free = 0
    }
  }
}

const installResumeHandlers = (socket: any) => {
  const state = stateOf(socket)

  // The proxy reports what it has accepted in its pong. That is the only thing
  // that frees retained output.
  socket.on('pong', (payload: string) => {
    const proxyRx = Number(String(payload).split(':')[1])
    if (Number.isFinite(proxyRx)) confirmTo(state, proxyRx)
  })

  attachSocketListeners(socket, socket._ws, state)
}

const attachSocketListeners = (socket: any, ws: WebSocket, state: ResumeState) => {
  ws.addEventListener('close', () => {
    if (state.closing || state.broken) return
    if (socket._ws !== ws) return // already replaced

    emit('lost', { attempts: state.attempts })
    void reconnect(socket, state)
  })
}

const delayFor = (attempts: number) => Math.min(RECONNECT_BASE_MS * 2 ** Math.max(0, attempts - 1), RECONNECT_MAX_MS)

const sleep = async (ms: number): Promise<void> => new Promise(resolve => {
  setTimeout(resolve, ms)
})

/** Wait for a network rather than burning attempts against a dead radio. */
const waitForOnline = async (): Promise<void> => {
  if (typeof navigator === 'undefined' || navigator.onLine) return
  return new Promise<void>(resolve => {
    window.addEventListener('online', () => {
      resolve()
    }, { once: true })
  })
}

/**
 * Dial a fresh WebSocket for the same session and re-attach it to the existing
 * Duplex.
 *
 * Nothing above the socket is touched, which is the entire point: the protocol
 * stack keeps its cipher position and half-read frame, and simply sees the byte
 * stream continue.
 */
const reconnect = async (socket: any, state: ResumeState) => {
  if (state.reconnecting || state.broken) return
  state.reconnecting = true

  while (!state.closing && !state.broken) {
    state.attempts++
    // eslint-disable-next-line no-await-in-loop -- backoff is inherently serial
    await sleep(delayFor(state.attempts))
    if (state.closing || state.broken) break

    // eslint-disable-next-line no-await-in-loop -- as above
    await waitForOnline()

    // State how much we already have, in the URL, so the proxy knows before it
    // sends anything. It replays everything unconfirmed the moment a transport
    // attaches; were our true count to arrive afterwards, the replay would start
    // from a stale acknowledgement and resend bytes we already had. A duplicate
    // corrupts the byte stream exactly as a gap does, and just as silently.
    const received: number = socket.bytesRead ?? 0
    const url = `${socket._resumeUrl}&received=${received}`

    // eslint-disable-next-line no-await-in-loop -- as above
    const ws = await openSocket(url)
    if (!ws) continue

    socket._ws = ws
    // net-browserify's handler pushes inbound frames into the Duplex and emits
    // 'pong'; reusing it keeps one code path for message handling.
    socket._handleWebsocket()
    attachSocketListeners(socket, ws, state)

    replay(socket, state)

    state.reconnecting = false
    state.attempts = 0
    state.resumes++
    emit('resumed', { resumes: state.resumes, replayed: state.pendingBytes })
    return
  }

  state.reconnecting = false
}

const openSocket = async (url: string): Promise<WebSocket | null> => new Promise(resolve => {
  let ws: WebSocket
  try {
    ws = new WebSocket(url)
  } catch {
    resolve(null)
    return
  }
  const settle = (value: WebSocket | null) => {
    ws.removeEventListener('open', onOpen)
    ws.removeEventListener('error', onError)
    resolve(value)
  }
  const onOpen = () => settle(ws)
  const onError = () => settle(null)
  ws.addEventListener('open', onOpen)
  ws.addEventListener('error', onError)
})

/**
 * Resend everything the proxy has not confirmed.
 *
 * Whether those bytes were actually lost is unknowable from here — only the
 * proxy knows what arrived — so everything unconfirmed goes again and the
 * proxy's own count sorts it out. Resending what it already has would duplicate
 * bytes in the stream, which is why it discards below its receive count rather
 * than trusting ours.
 */
const replay = (socket: any, state: ResumeState) => {
  const pending = [...state.pending]
  for (const chunk of pending) {
    try {
      socket._ws.send(chunk)
      state.txTotal += chunk.length
    } catch {
      return // the new socket died mid-replay; the close handler takes over
    }
  }
}
