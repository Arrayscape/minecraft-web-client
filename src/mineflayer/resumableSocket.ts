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

/**
 * How long to keep trying before admitting the session is gone.
 *
 * The proxy holds a detached session for its own deadline and then closes the
 * Minecraft connection; past that there is nothing left to resume onto and every
 * further attempt is refused. Without a bound the client would retry forever
 * against a session that no longer exists, which to the player looks exactly
 * like a hang.
 */
const RESUME_WINDOW_MS = 60_000

/**
 * Close codes that mean the peer closed deliberately rather than the network
 * failing underneath it: normal, going-away, and no-status (a close handshake
 * carrying no code is still a handshake). Anything else — 1006 above all — is a
 * connection that died, which is precisely what a resume is for.
 */
const PEER_LEFT_CODES = new Set([1000, 1001, 1005])

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
  /** Set once a transport has opened: before that, failures are connect errors. */
  connected: boolean
  /** When the current outage started, for the resume window. */
  lostAt: number | undefined
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
    connected: false,
    lostAt: undefined,
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
    installPongHandler(this)
    return result
  }

  // Replaces net-browserify's version wholesale rather than wrapping it. Its
  // handlers are written on the assumption that the socket has exactly one
  // WebSocket for its whole life, and three of them actively fight a resume:
  // the 'close' handler destroys the Duplex, the 'error' handler emits on it
  // (which destroys it a level up), and the connect timeout fires ten seconds
  // after any attach that did not come with a fresh 'open'.
  Socket.prototype._handleWebsocket = function () {
    attachTransport(this, this._ws, stateOf(this), true)
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
    giveUp(socket, state, `unconfirmed output passed ${MAX_UNCONFIRMED_BYTES} bytes`)
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

/** The session cannot be continued. Say so once, and let the socket close. */
const giveUp = (socket: any, state: ResumeState, reason: string) => {
  if (state.broken) return
  state.broken = true
  console.warn(`[resume] ${reason}; this session can no longer be resumed`)
  emit('unresumable', { reason })
  if (socket?.readyState === 'open') socket.destroy()
}

const installPongHandler = (socket: any) => {
  if (socket._resumePong) return
  socket._resumePong = true

  // The proxy reports what it has accepted in its pong. That is the only thing
  // that frees retained output.
  socket.on('pong', (payload: string) => {
    const proxyRx = Number(String(payload).split(':')[1])
    if (Number.isFinite(proxyRx)) confirmTo(stateOf(socket), proxyRx)
  })
}

/**
 * Wire a WebSocket to the Duplex.
 *
 * `initial` marks the socket net-browserify dialled during connect(): only that
 * one announces itself with 'connect' and is held to a connect timeout. A
 * resumed transport must do neither — the protocol stack above already believes
 * it is connected, and re-announcing would have it redo a handshake mid-session.
 */
/**
 * Tell the proxy where we are in its stream, before it sends anything.
 *
 * Every connection states it, the first one included, where it is zero. The
 * proxy has no safe value to assume in its place: resuming from the last
 * acknowledgement would replay everything since — bytes we already hold — and a
 * duplicate corrupts the byte stream exactly as thoroughly as a gap. Nothing on
 * this side would catch it, because `bytesRead` is a count, not a filter.
 */
const sendResumeOffset = (socket: any, ws: WebSocket) => {
  ws.send(`resume:${socket.bytesRead ?? 0}`)
}

const attachTransport = (socket: any, ws: WebSocket, state: ResumeState, initial: boolean) => {
  // Deliver binary frames synchronously. Asking for Blobs (the default) means
  // reading each one through a FileReader, so delivery becomes asynchronous per
  // frame and nothing orders the completions of two frames that arrive back to
  // back. A byte stream cannot survive reordering, and the corruption would
  // surface far from here as a desynchronised cipher.
  ws.binaryType = 'arraybuffer'

  let settled = !initial
  let timeout: any

  if (initial) {
    timeout = setTimeout(() => {
      if (settled) return
      settled = true
      ws.close()
      socket.emit('error', `Proxy server is reachable, but the WebSocket connection timed out after ${socket._wsTimeout / 1000} seconds. Possible reasons:
1. Most probably the proxy server (${hostOf(ws)}) is misconfigured and not accepting WebSocket connections.
2. Your browser or network is blocking WebSocket connections.`)
    }, socket._wsTimeout)

    ws.addEventListener('open', () => {
      if (settled) return
      settled = true
      clearTimeout(timeout)

      // Before 'connect', so nothing above can write ahead of the handshake.
      sendResumeOffset(socket, ws)

      state.connected = true
      socket._connecting = false
      socket.readable = true
      socket.emit('connect')
      socket.read(0)
    })
  } else {
    state.connected = true
  }

  ws.addEventListener('error', (e: any) => {
    if (socket._ws !== ws) return
    if (!state.connected) {
      // Nothing ever came up, so this is a connect failure and the player needs
      // to hear about it.
      if (settled) return
      settled = true
      clearTimeout(timeout)
      console.warn('TCP error', e)
      socket.emit('error', 'An error occurred with the WebSocket connection. Please check your network connection and proxy server status.')
      return
    }
    // An established transport fails by closing, and 'error' arrives first.
    // Emitting it on the Duplex would destroy the very state the resume exists
    // to preserve; the close handler decides what happens next.
    if (state.closing || state.broken) return // shutting down: not worth a word
    console.warn('[resume] transport error; waiting for close')
  })

  ws.addEventListener('message', (e: MessageEvent) => {
    const contents = e.data

    if (typeof contents === 'string') {
      if (contents.startsWith('pong:')) {
        socket.emit('pong', contents.slice('pong:'.length))
        return
      }
      if (contents === 'ackreq') {
        // The proxy is asking where we are, because its buffer is filling and
        // nothing is released until we say. It knows how full it is and we do
        // not, so it asks and we answer immediately rather than waiting for the
        // heartbeat — which is a timer chosen for latency reporting, not for
        // how fast the server happens to be sending.
        //
        // The ordinary heartbeat frame is the answer: one way of stating a
        // position, prompted or not.
        try {
          ws.send(`ping:0:${socket.bytesRead ?? 0}`)
        } catch { /* the socket died between the request and the reply */ }
        return
      }
      if (socket.handleStringMessage(contents)) {
        deliver(socket, Buffer.from(contents))
      }
      return
    }
    if (contents instanceof ArrayBuffer) {
      deliver(socket, Buffer.from(contents))
      return
    }
    console.warn('Cannot read TCP stream: unsupported message type', contents)
  })

  ws.addEventListener('close', (e: any) => {
    if (socket._ws !== ws) return // already replaced

    const peerLeft = PEER_LEFT_CODES.has(e?.code)
    // Read before destroying: destroy() is patched to set `closing`, so asking
    // afterwards always says the close was ours.
    const endedHere = state.closing || state.broken
    if (endedHere || peerLeft) {
      // Deliberate on one side or the other: this is the end of the session, and
      // the Duplex should end with it.
      const wasOpen = socket.readyState === 'open'
      if (wasOpen) socket.destroy()

      // net-browserify's destroy only sets flags — it emits nothing — so a far
      // end that hangs up cleanly leaves the protocol stack above waiting for a
      // packet that will never come. minecraft-protocol ends the client on
      // 'close'; say it, so a real disconnect reaches the player instead of the
      // session quietly going still.
      //
      // Only when the other side left: a local end() or destroy() has already
      // told everything above, and 'unresumable' carries the giving-up case.
      if (wasOpen && peerLeft && !endedHere) socket.emit('close')
      return
    }

    state.lostAt ??= Date.now()
    emit('lost', { attempts: state.attempts })
    void reconnect(socket, state)
  })
}

/** The proxy's host, for a connect failure the player has to act on. */
const hostOf = (ws: WebSocket): string => {
  try {
    return new URL(ws.url).host
  } catch {
    return 'unknown host'
  }
}

const deliver = (socket: any, buf: Buffer) => {
  // bytesRead is what the heartbeat reports to the proxy, so it must count
  // bytes as they are pushed: data sitting in the Duplex has been received,
  // whether or not the decipher has consumed it yet.
  socket.bytesRead += buf.length
  socket.push(buf)
}

const delayFor = (attempts: number) => Math.min(RECONNECT_BASE_MS * 2 ** Math.max(0, attempts - 1), RECONNECT_MAX_MS)

const sleep = async (ms: number): Promise<void> => new Promise(resolve => {
  setTimeout(resolve, ms)
})

/** Wait for a network rather than burning attempts against a dead radio. */
const waitForOnline = async (): Promise<void> => {
  // Only an explicit false means offline. Where the browser does not report it
  // at all, assume a network and let the dial decide.
  const offline = typeof navigator !== 'undefined' && (navigator as any).onLine === false
  if (!offline) return
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
    if (Date.now() - (state.lostAt ?? Date.now()) > RESUME_WINDOW_MS) {
      giveUp(socket, state, `no transport for ${Math.round(RESUME_WINDOW_MS / 1000)}s`)
      break
    }

    state.attempts++
    // eslint-disable-next-line no-await-in-loop -- backoff is inherently serial
    await sleep(delayFor(state.attempts))
    if (state.closing || state.broken) break

    // eslint-disable-next-line no-await-in-loop -- as above
    await waitForOnline()

    // eslint-disable-next-line no-await-in-loop -- as above
    const ws = await openSocket(socket._resumeUrl)
    if (!ws) continue

    socket._ws = ws
    attachTransport(socket, ws, state, false)

    // Both positions, in order, before any data: where we are in the proxy's
    // stream, then where our own replay begins. Nothing can interleave — this
    // runs to completion without yielding.
    sendResumeOffset(socket, ws)
    replay(socket, state)

    state.reconnecting = false
    state.attempts = 0
    state.lostAt = undefined
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
    ws.removeEventListener('close', onClose)
    resolve(value)
  }
  const onOpen = () => settle(ws)
  const onError = () => settle(null)
  // A socket refused before it opens closes without ever erroring in some
  // browsers; without this the dial would never settle and the resume would
  // stall on an attempt that already failed.
  const onClose = () => settle(null)
  ws.addEventListener('open', onOpen)
  ws.addEventListener('error', onError)
  ws.addEventListener('close', onClose)
})

/**
 * Resend everything the proxy has not confirmed.
 *
 * Whether those bytes were actually lost is unknowable from here — only the
 * proxy knows what arrived — so everything unconfirmed goes again, prefixed by
 * where the replay starts so the proxy can drop what it already has.
 *
 * That prefix is not optional. Our idea of what the proxy accepted comes from
 * its pong replies and is always behind the truth: everything sent since the
 * last one has to be assumed lost. So the replay necessarily overlaps what the
 * proxy already forwarded to Minecraft, and a duplicated byte range corrupts
 * that stream exactly as thoroughly as a gap — the server reads a bogus packet
 * length and closes the connection.
 *
 * Live writes cannot overtake it: the caller attaches, hands over both offsets
 * and replays without yielding, so nothing else runs in between.
 */
const replay = (socket: any, state: ResumeState) => {
  try {
    socket._ws.send(`sent:${state.proxyRx}`)
  } catch {
    return // the new socket died before the replay began
  }

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
