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
// Byte accounting is the proxy's, mirrored. Every offset is absolute — bytes
// since the stream began — and there are two streams, one per direction:
//
//   bytesRead     our position in the proxy's stream: what we have received.
//                 Stated in `resume:` and in every ping and pong.
//   out.acked     the proxy's position in ours: what it has accepted. Everything
//                 after it is retained and replayed on reconnect.
//   out.produced  our own position in our stream. Nothing the proxy says may
//                 exceed it.
//
// A send is not a delivery. Bytes sitting in a dying socket's buffer are lost
// with no error anywhere, so only the peer's own offset may free anything.
//
// The window itself is `StreamBuffer`, the same component the proxy runs, with
// the same positions and the same refusals. See streamBuffer.ts for the one
// place the two implementations are allowed to differ.

import net from 'net'
import { StreamBuffer, StreamPositionError } from './streamBuffer'

/** Cap on retained outbound bytes. Past this a resume cannot be honest. */
const MAX_UNCONFIRMED_BYTES = 4 * 1024 * 1024

/**
 * Retained bytes past which we ask the proxy where it is, rather than waiting
 * for the next heartbeat.
 *
 * The mirror of the proxy's `ackreq`, and on this side it is ours to decide for
 * the same reason it is theirs to decide over there: only the holder of a buffer
 * knows how full it is. The proxy cannot see this number and should not be
 * guessing at it.
 */
const ASK_FOR_ACK_BYTES = MAX_UNCONFIRMED_BYTES / 2

/**
 * How much we are willing to leave sitting in the browser's own send queue.
 *
 * `WebSocket.send` never blocks and never fails on a slow link: the browser
 * copies the bytes into a queue the spec puts no bound on, with no drain event
 * and nothing observable but `bufferedAmount`. Feeding it without looking means
 * the cursor advances past what the transport has actually accepted, the same
 * bytes are held twice — once in our window, once in that queue — and congestion
 * becomes invisible.
 *
 * The proxy has no equivalent because Go's write blocks, so its cursor tracks
 * what the transport took. This is how ours comes to mean the same thing.
 *
 * Sized so that ordinary play never reaches it — upstream is a few KB/s — while
 * a replay after a long outage goes out in a handful of passes rather than one
 * unbounded shove.
 */
const MAX_BUFFERED_BYTES = 64 * 1024

/** How soon to look again while the queue is above that. */
const PUMP_RETRY_MS = 25

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

/**
 * Close codes the proxy uses to say a session cannot be resumed.
 *
 * These exist because a browser learns nothing from a rejected upgrade — no
 * status, no body, and a 1006 that means "the connection failed", which is
 * exactly what an unreachable proxy looks like. Without them a client whose
 * session had been torn down would retry until its own window expired, leaving
 * the player watching a game that had already ended.
 *
 * 4003 (busy) is deliberately not here: the previous transport has not released
 * the session yet, and the next attempt is expected to work.
 */
const TERMINAL_CODES = new Map<number, string>([
  [4004, 'the session no longer exists'],
  [4001, 'the proxy refused to resume from our position'],
  [4002, 'the proxy rejected our resume handshake'],
])

export interface ResumeState {
  /**
   * The window over what we are sending. Head, cursor and produced, with the
   * same operations and the same refusals as the proxy's — see streamBuffer.ts.
   */
  out: StreamBuffer
  /** Set when the stream can no longer be resumed honestly. */
  broken: boolean
  reconnecting: boolean
  attempts: number
  resumes: number
  /** Set by end()/destroy() so a deliberate close is not treated as a drop. */
  closing: boolean
  /** Set once the proxy has been asked where it is, cleared when it answers. */
  ackAsked: boolean
  /** Numbers our own pings; the proxy echoes it back. */
  pingSeq: number
  /**
   * Set only while the transport is congested and bytes are waiting.
   *
   * There is no drain event to wait on, so this is what wakes the pump. It
   * exists only while it has work: in ordinary operation the writes themselves
   * drive everything and no timer is created at all.
   */
  pumpTimer: any
  /**
   * Whether the window is bound to a transport.
   *
   * The same rule the proxy applies: a transport carries nothing until the
   * peer's position is known, because sending from our own estimate of it would
   * re-send bytes the peer already holds.
   */
  attached: boolean
  /** Set once a transport has opened: before that, failures are connect errors. */
  connected: boolean
  /** When the current outage started, for the resume window. */
  lostAt: number | undefined
}

const stateOf = (socket: any): ResumeState => {
  socket._resume ??= {
    out: new StreamBuffer(MAX_UNCONFIRMED_BYTES),
    broken: false,
    reconnecting: false,
    attempts: 0,
    resumes: 0,
    closing: false,
    ackAsked: false,
    pingSeq: 0,
    pumpTimer: undefined,
    attached: false,
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

    if (!state.broken && !state.out.write(buf)) {
      // The window is full. The proxy's answer here is to stall its producer,
      // which closes the TCP window and stops Minecraft sending. Ours cannot be:
      // this producer is mineflayer's physics loop, and stalling it freezes the
      // player, which is what resumption exists to prevent. So the session ends
      // instead.
      giveUp(this, state, `unconfirmed output passed ${MAX_UNCONFIRMED_BYTES} bytes`)
    }

    pump(this, state)
    askForAckIfFilling(this, state)

    // Reporting success is deliberate even when nothing went out. Applying
    // backpressure here would stall the physics loop; the player's actions queue
    // instead of being lost.
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

/**
 * Move whatever the window has at its cursor onto the transport.
 *
 * The client's half of `DurableStream.serve`. It runs on writes and on attach
 * rather than in a loop of its own, because in a browser the producer and the
 * consumer are the same thread and there is nothing to wait on: a write is the
 * only thing that creates work.
 */
const pump = (socket: any, state: ResumeState) => {
  if (!state.attached) return

  const ws = socket._ws
  if (!ws || ws.readyState !== WebSocket.OPEN) return

  for (;;) {
    if (state.out.unsent === 0) return

    // Stop feeding a queue that is not keeping up, and leave the bytes where
    // they are. This is the client's version of the proxy blocking in its
    // write: the window fills, and if it fills completely the session ends —
    // rather than the browser silently swallowing an unbounded copy of it.
    if (ws.bufferedAmount > MAX_BUFFERED_BYTES) {
      schedulePump(socket, state)
      return
    }

    const chunk = state.out.next()
    if (!chunk) return
    try {
      ws.send(chunk)
    } catch {
      // The transport died mid-send. The cursor is now ahead of what the proxy
      // received; the resync at the next attach is what puts it back.
      return
    }
  }
}

/** Look again shortly. Idempotent: one pending timer at a time. */
const schedulePump = (socket: any, state: ResumeState) => {
  if (state.pumpTimer !== undefined) return
  state.pumpTimer = setTimeout(() => {
    state.pumpTimer = undefined
    pump(socket, state)
  }, PUMP_RETRY_MS)
}

const stopPump = (state: ResumeState) => {
  if (state.pumpTimer === undefined) return
  clearTimeout(state.pumpTimer)
  state.pumpTimer = undefined
}

/**
 * Apply a position the proxy has stated.
 *
 * `rewind` distinguishes the two frames that carry one: a ping or pong only
 * frees, a resume also sends the cursor back so everything after it goes again.
 * Both refusals are the proxy's, mirrored — a peer cannot have received what was
 * never sent to it, nor ask for bytes already released.
 */
const acceptPosition = (socket: any, state: ResumeState, offset: number, rewind: boolean) => {
  if (!Number.isFinite(offset) || state.broken) return

  try {
    if (rewind) state.out.resync(offset)
    else state.out.ack(offset)
  } catch (err) {
    if (err instanceof StreamPositionError) {
      giveUp(socket, state, err.message)
      return
    }
    throw err
  }
  state.ackAsked = false
}

/**
 * Ask the proxy where it is, once we are holding too much.
 *
 * The mirror of the proxy's own prompt, and ours to decide for the same reason
 * theirs is theirs: only the holder of a buffer knows how full it is. Useful
 * only while there is a transport — during an outage there is nobody to ask, and
 * that is when this grows fastest.
 */
const askForAckIfFilling = (socket: any, state: ResumeState) => {
  if (state.ackAsked || state.out.length < ASK_FOR_ACK_BYTES) return

  const ws = socket._ws
  if (!ws || ws.readyState !== WebSocket.OPEN) return

  state.ackAsked = true
  try {
    ws.send(`ping:${++state.pingSeq}:${socket.bytesRead ?? 0}`)
  } catch {
    state.ackAsked = false
  }
}

/** The session cannot be continued. Say so once, and let the socket close. */
const giveUp = (socket: any, state: ResumeState, reason: string) => {
  if (state.broken) return
  state.broken = true
  stopPump(state)
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
    acceptPosition(socket, stateOf(socket), Number(String(payload).split(':')[1]), false)
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

      // Not attached yet: like every other attach, this one waits for the peer
      // to say where it is. It costs one one-way latency on a connection that
      // has just done an HTTP round trip to /connect anyway, and it means there
      // is one rule rather than a rule and an exception.
      //
      // Attaching optimistically at zero looks safe — a fresh session really is
      // at zero on both sides — but the peer's frame then arrives after bytes
      // are already in flight, and applying its rewind resends them.
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
      if (contents.startsWith('resume:')) {
        acceptResume(socket, state, Number(contents.slice('resume:'.length)))
        return
      }
      if (contents.startsWith('pong:')) {
        // A reply to our ping. Its offset frees retained output exactly as a
        // ping's does; the event is what the latency plugin listens for.
        acceptPosition(socket, state, Number(contents.slice('pong:'.length).split(':')[1]), false)
        socket.emit('pong', contents.slice('pong:'.length))
        return
      }
      if (contents.startsWith('ping:')) {
        // The proxy asking where we are, because its buffer is filling and
        // nothing is released until we say. We do the same to it — same frame,
        // same meaning, opposite direction.
        const [seq, offset] = contents.slice('ping:'.length).split(':')
        acceptPosition(socket, state, Number(offset), false)
        try {
          ws.send(`pong:${seq}:${socket.bytesRead ?? 0}`)
        } catch { /* the socket died between the ping and the reply */ }
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
    state.attached = false
    stopPump(state) // it belongs to the transport that is going away

    const terminal = TERMINAL_CODES.get(e?.code)
    if (terminal) {
      giveUp(socket, state, e?.reason || terminal)
      return
    }

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

/**
 * The proxy stating where it is, at attach.
 *
 * This is the whole resume in one frame: release everything it has, rewind to
 * there, and send. Because the offset is the proxy's own truth rather than our
 * estimate of it, nothing we send can be a byte it already holds — so there is
 * nothing for it to discard, and the replay is exactly once.
 */
const acceptResume = (socket: any, state: ResumeState, offset: number) => {
  if (state.attached) {
    // One per connection, at the start. A second would rewind a cursor whose
    // in-flight bytes are not lost, and resend them.
    console.warn('[resume] ignoring a second resume frame on an attached transport')
    return
  }

  acceptPosition(socket, state, offset, true)
  if (state.broken) return

  state.attached = true
  pump(socket, state)
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
    const dialled = await openSocket(socket._resumeUrl)
    if (!(dialled instanceof WebSocket)) {
      // Retrying is only worth it if there is something to come back to. When
      // the proxy has said there is not, say so now rather than after the
      // window expires: the player is waiting to be told.
      const terminal = TERMINAL_CODES.get(dialled.code)
      if (terminal) {
        giveUp(socket, state, dialled.reason || terminal)
        break
      }
      continue
    }
    const ws = dialled

    socket._ws = ws
    // Not attached until the proxy says where it is: sending from our own
    // estimate would re-send bytes it already holds, and it has nothing to
    // discard them with. The proxy applies the same rule to us.
    state.attached = false
    attachTransport(socket, ws, state, false)
    sendResumeOffset(socket, ws)

    state.reconnecting = false
    state.attempts = 0
    state.lostAt = undefined
    state.resumes++
    emit('resumed', { resumes: state.resumes, replayed: state.out.length })
    return
  }

  state.reconnecting = false
}

/** A dial that failed, and what the far end said about it. */
interface DialFailure { code: number, reason: string }

const openSocket = async (url: string): Promise<WebSocket | DialFailure> => new Promise(resolve => {
  let ws: WebSocket
  try {
    ws = new WebSocket(url)
  } catch {
    resolve({ code: 0, reason: 'the socket could not be created' })
    return
  }

  const settle = (value: WebSocket | DialFailure) => {
    ws.removeEventListener('open', onOpen)
    ws.removeEventListener('error', onError)
    ws.removeEventListener('close', onClose)
    resolve(value)
  }
  const onOpen = () => settle(ws)
  const onError = () => settle({ code: 0, reason: 'the socket could not be opened' })
  // A socket refused before it opens closes without ever erroring in some
  // browsers, and the close carries the only explanation there is going to be.
  const onClose = (e: any) => settle({ code: e?.code ?? 0, reason: e?.reason ?? '' })

  ws.addEventListener('open', onOpen)
  ws.addEventListener('error', onError)
  ws.addEventListener('close', onClose)
})
