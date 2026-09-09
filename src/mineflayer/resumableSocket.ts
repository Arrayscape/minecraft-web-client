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
/**
 * How long a transport may go without a single frame before we stop believing
 * in it.
 *
 * The browser hands JavaScript no way to see a socket that has stopped
 * delivering: WebSocket ping/pong is answered below the page and never surfaces,
 * and a black-holed TCP connection produces no event until the kernel gives up.
 * Measured in the field that took 73 seconds, against a proxy that noticed in
 * 30 — so by the time the page reacted its session had already been collected
 * and the resume was impossible. A dropped Wi-Fi interface is signalled at once
 * and hides this; a venue network that black-holes, a NAT rebind on an access
 * point roam, and a captive portal that holds the connection open while
 * forwarding nothing do not.
 *
 * So the proxy beats every WSPingPeriod (10s, compiled in) with a `ping:` frame
 * the page *can* see, and this is three of those — the same three-missed-beats
 * rule the proxy applies to us with WSPongWait. Raising the proxy's period
 * without raising this would make healthy sessions trip it.
 *
 * Deliberately not measured against Minecraft's own traffic: that would confuse
 * a dead link with a stalled server, and reconnecting because the game went
 * quiet costs a resume and fixes nothing.
 */
const SILENCE_LIMIT_MS = 30_000

/**
 * How long after the last frame the session is still worth chasing.
 *
 * Measured from the last frame rather than from the moment the loss was
 * noticed, because those are not the same instant and the difference is the
 * whole watchdog: noticing now takes 30s, so a window anchored to it ran to 90s
 * against a proxy that had let go at 55.
 *
 * What bounds it is the *upstream*, not the proxy. Velocity's read-timeout and
 * Paper's keep-alive both give 30s from the last inbound client data, after
 * which the player has been dropped and no reconnection can produce a playable
 * session — only a better-worded disconnect. Chasing past that point keeps the
 * player staring at a frozen world hoping, which is worse than telling them.
 *
 * 40s rather than 30s because both clocks are staler than they look. The last
 * frame we saw can be up to a heartbeat (10s) before the outage, since an idle
 * world sends nothing else; Velocity's can be up to a keep-alive interval (15s)
 * before it, for the mirror reason. From here the upstream therefore dies
 * somewhere in lastAliveAt + [15, 40]. Giving up sooner than 40 could abandon a
 * session that was still playable; at 40 it certainly is not.
 *
 * It is only a backstop. While the proxy is reachable it answers for itself with
 * a 4004, which is both faster and better worded than anything inferred here;
 * this covers the case where it cannot be reached at all. The proxy collects its
 * side at WSPongWait + ResumeDeadline = 40s too, so the two can race — benignly,
 * since either way the player is correctly told the session ended.
 */
const RESUME_WINDOW_MS = 40_000

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
  /** Polls the clock for both deadlines; see tickWatch. */
  watchTimer: any
  /** The visibilitychange listener, so it can be removed with the timer. */
  onVisible?: () => void
  /**
   * When the transport last showed evidence of life — a frame, or the moment it
   * opened. The instant the proxy's own budget runs from.
   */
  lastAliveAt?: number
  /**
   * Set while a reattach is in flight, so that success is declared when the
   * peer answers rather than when the socket opens.
   */
  resuming: boolean
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
    watchTimer: undefined,
    lastAliveAt: undefined,
    resuming: false,
    attached: false,
    connected: false,
    lostAt: undefined,
  } satisfies ResumeState
  return socket._resume
}

/** Resume state for a socket, for the UI and for tests. */
export const getResumeState = (socket: any): ResumeState | undefined => socket?._resume

/**
 * Trace the protocol as this end speaks it.
 *
 * The negotiation is one exchange per connection, so it is always logged: when
 * something goes wrong here it goes wrong once, at attach, and the two offsets
 * involved are the whole story. `localStorage.resumeTrace = '1'` adds the
 * heartbeat, which is every few seconds and drowns that story otherwise.
 *
 * Arrows are from this end's point of view: `->` sent, `<-` received.
 */
const chatty = (() => {
  try {
    return localStorage.getItem('resumeTrace') === '1'
  } catch {
    return false // storage can throw in a private window
  }
})()

// Elapsed seconds since the module loaded. Every line the resume path prints is
// about *when* something happened relative to something else — how long silence
// lasted, how long a dial hung — and the browser console does not stamp its
// output. Without this a log can be read two incompatible ways and neither can
// be ruled out, which is exactly what happened to the first blackout run.
const started = Date.now()
const since = () => `+${((Date.now() - started) / 1000).toFixed(1)}s`

const trace = (...args: any[]) => console.log('[resume]', since(), ...args)
const traceChatty = (...args: any[]) => {
  if (chatty) trace(...args)
}

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
 * Both of this file's deadlines, enforced by polling the clock rather than by a
 * long timer.
 *
 * A `setTimeout` is the obvious way to express "act in 30 seconds", and it is the
 * wrong one in a browser. A hidden tab has its timers throttled to one a second,
 * and after five minutes hidden to roughly one a minute, so a long timeout armed
 * into an idle background page may not run anywhere near when it was asked to.
 * Measured: with the tab focused the watchdog fired 30.0s after the last frame;
 * with the tab hidden behind a terminal it did not fire at all, and the session
 * was reported only when the browser eventually produced a close — the exact
 * 73-second blindness this was built to remove.
 *
 * That is not a test artifact. Players tab away, and a player who does so during
 * an outage is precisely the one waiting to be told.
 *
 * Polling is robust to throttling in the way a timeout is not: whichever tick
 * happens to run compares wall-clock times and reaches the right conclusion, so
 * throttling delays detection instead of defeating it. `visibilitychange` then
 * closes the remaining gap — a returning tab is checked at once rather than at
 * the next tick.
 *
 * One timer covers both deadlines because they are the same question asked of
 * different states: how long since this socket last proved it was carrying, and
 * is that longer than what we are currently willing to wait.
 */
const tickWatch = (socket: any, state: ResumeState) => {
  if (state.closing || state.broken) {
    stopWatch(state)
    return
  }
  const quiet = Date.now() - (state.lastAliveAt ?? Date.now())

  if (state.attached) {
    // Overridable per socket so a test can run this at millisecond scale, the
    // way _wsTimeout already works for the connect timeout.
    const limit: number = socket._silenceLimit ?? SILENCE_LIMIT_MS
    if (quiet < limit) return

    trace(`no frame for ${(quiet / 1000).toFixed(1)}s; presuming the transport is dead`)

    // Do not wait for the close event this close() will eventually produce. A
    // close handshake on a link that is already swallowing packets is answered
    // by nobody, and the browser sits on it — which is the exact delay this
    // watchdog exists to cut short. Mark the socket so that when the event does
    // arrive it is ignored rather than starting a second reconnect.
    const ws = socket._ws
    if (ws) {
      ;(ws)._abandoned = true
      try {
        ws.close()
      } catch { /* already gone; the reconnect below is what matters */ }
    }
    loseTransport(socket, state)
    return
  }

  // Detached: the retry loop is running, or parked. Either way the session stops
  // being worth chasing at the point the upstream has certainly dropped the
  // player, and nothing in the loop can be relied on to notice — it parks in
  // waitForOnline while the browser reports no network, and in a dial that a
  // black-holed link will not fail quickly.
  const window: number = socket._resumeWindow ?? RESUME_WINDOW_MS
  if (quiet >= window) {
    giveUp(socket, state, `no transport for ${Math.round(window / 1000)}s`)
  }
}

/**
 * How often to compare the clock. Fine enough to be punctual, coarse enough to
 * be free.
 *
 * Derived from whichever deadline is shorter, since either may be the one in
 * force: a tick slower than the threshold it is meant to catch would report it
 * late by up to a whole interval.
 */
const watchIntervalFor = (socket: any): number => {
  const limit: number = socket._silenceLimit ?? SILENCE_LIMIT_MS
  const window: number = socket._resumeWindow ?? RESUME_WINDOW_MS
  return Math.max(50, Math.min(Math.floor(Math.min(limit, window) / 4), 5000))
}

const startWatch = (socket: any, state: ResumeState) => {
  if (state.watchTimer !== undefined || state.closing || state.broken) return
  state.watchTimer = setInterval(() => {
    tickWatch(socket, state)
  }, watchIntervalFor(socket))

  // A tab coming back to the foreground has usually just had its timers
  // throttled; check immediately rather than making the player wait for a tick.
  if (typeof document !== 'undefined' && state.onVisible === undefined) {
    state.onVisible = () => {
      if (document.visibilityState === 'visible') tickWatch(socket, state)
    }
    document.addEventListener('visibilitychange', state.onVisible)
  }
}

const stopWatch = (state: ResumeState) => {
  if (state.watchTimer !== undefined) {
    clearInterval(state.watchTimer)
    state.watchTimer = undefined
  }
  if (state.onVisible !== undefined && typeof document !== 'undefined') {
    document.removeEventListener('visibilitychange', state.onVisible)
    state.onVisible = undefined
  }
}

/** Record that the transport just proved it is carrying something. */
const noteAlive = (state: ResumeState) => {
  state.lastAliveAt = Date.now()
}

/**
 * Give up on the current transport and start looking for another.
 *
 * Shared by the close event and the silence watchdog, because a socket that
 * closed and a socket that stopped speaking are the same situation: this side
 * still holds everything the peer has not confirmed, and the session outlives
 * the connection carrying it.
 */
const loseTransport = (socket: any, state: ResumeState) => {
  state.attached = false
  stopPump(state)
  state.lostAt ??= Date.now()
  // The watcher is not stopped here: detached is exactly when its other
  // deadline applies. lastAliveAt is left alone so the window runs from the
  // last frame rather than from when the loss was noticed.
  startWatch(socket, state)
  trace(`transport lost; holding ${state.out.length} bytes, will reconnect`)
  emit('lost', { attempts: state.attempts })
  void reconnect(socket, state)
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
    trace(`-> ping:${state.pingSeq}:${socket.bytesRead ?? 0}`,
      `(holding ${state.out.length} bytes unconfirmed; asking where they are)`)
  } catch {
    state.ackAsked = false
  }
}

/** The session cannot be continued. Say so once, and let the socket close. */
const giveUp = (socket: any, state: ResumeState, reason: string) => {
  if (state.broken) return
  state.broken = true
  state.resuming = false
  stopPump(state)
  stopWatch(state)
  console.warn(`[resume] ${since()} ${reason}; this session can no longer be resumed`)

  // Say why before tearing anything down, so whatever is listening sets the
  // reason the player sees. What follows announces the end in the ordinary way,
  // and the first end wins.
  emit('unresumable', { reason })

  const wasOpen = socket?.readyState === 'open'
  if (wasOpen) socket.destroy()

  // Tell the protocol stack the socket is gone. Without this nothing above
  // stops writing: the physics loop keeps going, hits a destroyed Duplex, and
  // "Cannot call write after a stream was destroyed" surfaces as an uncaught
  // protocol error — with the real reason nowhere in sight.
  if (wasOpen) socket.emit('close')
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
  const at = socket.bytesRead ?? 0
  trace(`-> resume:${at}`, at === 0 ? '(nothing received yet)' : '(bytes of theirs we hold)')
  ws.send(`resume:${at}`)
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

      // From here a silent socket is a dead one — including a peer that accepts
      // the connection and never states its position, which would otherwise
      // leave this side attached to nothing for as long as it cared to wait.
      noteAlive(state)
      startWatch(socket, state)
    })
  } else {
    state.connected = true
    // Already open: openSocket waited for that before handing it over.
    noteAlive(state)
    startWatch(socket, state)
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
    // Anything at all proves the transport is carrying. Before the type is even
    // looked at, because a control frame counts exactly as much as a chunk of
    // world data for this purpose.
    noteAlive(state)

    const contents = e.data

    if (typeof contents === 'string') {
      if (contents.startsWith('resume:')) {
        acceptResume(socket, state, Number(contents.slice('resume:'.length)))
        return
      }
      if (contents.startsWith('pong:')) {
        // A reply to our ping. Its offset frees retained output exactly as a
        // ping's does; the event is what the latency plugin listens for.
        traceChatty(`<- ${contents}`, `(they hold ${state.out.length} of ours unconfirmed)`)
        acceptPosition(socket, state, Number(contents.slice('pong:'.length).split(':')[1]), false)
        socket.emit('pong', contents.slice('pong:'.length))
        return
      }
      if (contents.startsWith('ping:')) {
        // The proxy asking where we are, because its buffer is filling and
        // nothing is released until we say. We do the same to it — same frame,
        // same meaning, opposite direction.
        const [seq, offset] = contents.slice('ping:'.length).split(':')
        traceChatty(`<- ${contents}`, '(heartbeat, or their buffer is filling; either way they want our position)')
        acceptPosition(socket, state, Number(offset), false)
        try {
          ws.send(`pong:${seq}:${socket.bytesRead ?? 0}`)
          traceChatty(`-> pong:${seq}:${socket.bytesRead ?? 0}`)
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
    // The watchdog already declared this one dead and moved on.
    if ((ws as any)._abandoned) return
    state.attached = false
    stopPump(state) // it belongs to the transport that is going away

    trace(`closed code=${e?.code ?? '?'}`, e?.reason ? `reason=${JSON.stringify(e.reason)}` : '(no reason)')

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

    loseTransport(socket, state)
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

  const heldBefore = state.out.acked
  acceptPosition(socket, state, offset, true)
  if (state.broken) return

  const toReplay = state.out.unsent
  trace(`<- resume:${offset}`,
    `(released ${offset - heldBefore}, replaying ${toReplay} bytes from ${offset})`)

  state.attached = true
  pump(socket, state)

  // A resume is complete when the peer has stated its position and this side is
  // sending again — not when the socket opened. Declaring it at dial time meant
  // announcing success to a session the proxy was about to refuse, and counting
  // it.
  if (state.resuming) {
    state.resuming = false
    state.resumes++
    emit('resumed', { resumes: state.resumes, replayed: toReplay })
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
    if (Date.now() - (state.lastAliveAt ?? state.lostAt ?? Date.now()) > RESUME_WINDOW_MS) {
      giveUp(socket, state, `no transport for ${Math.round(RESUME_WINDOW_MS / 1000)}s`)
      break
    }

    state.attempts++
    // eslint-disable-next-line no-await-in-loop -- backoff is inherently serial
    await sleep(delayFor(state.attempts))
    if (state.closing || state.broken) break

    // eslint-disable-next-line no-await-in-loop -- as above
    await waitForOnline()

    trace(`dialling (attempt ${state.attempts}, ${Math.round((Date.now() - (state.lostAt ?? Date.now())) / 1000)}s down)`)
    // eslint-disable-next-line no-await-in-loop -- as above
    const dialled = await openSocket(socket._resumeUrl)

    // A dial has no deadline of its own, so it can return long after the session
    // stopped being worth having — a SYN sitting in a blackhole outlives the
    // resume window by however long the network stays down. Whatever came back
    // is now for a session nobody is holding.
    //
    // The successful case is the one that matters: without this it would be an
    // open WebSocket to the proxy that nothing owns and nothing will ever close.
    if (state.closing || state.broken) {
      if (dialled instanceof WebSocket) {
        trace('dial succeeded after the session was given up; closing it')
        try {
          dialled.close()
        } catch { /* nothing left to do about it */ }
      } else {
        trace(`dial failed code=${dialled.code} ${dialled.reason}; not retrying, the session is gone`)
      }
      break
    }

    if (!(dialled instanceof WebSocket)) {
      // Retrying is only worth it if there is something to come back to. When
      // the proxy has said there is not, say so now rather than after the
      // window expires: the player is waiting to be told.
      const terminal = TERMINAL_CODES.get(dialled.code)
      if (terminal) {
        giveUp(socket, state, dialled.reason || terminal)
        break
      }
      trace(`dial failed code=${dialled.code} ${dialled.reason}; retrying`)
      continue
    }
    const ws = dialled

    socket._ws = ws
    // Not attached until the proxy says where it is: sending from our own
    // estimate would re-send bytes it already holds, and it has nothing to
    // discard them with. The proxy applies the same rule to us.
    state.attached = false
    state.resuming = true
    attachTransport(socket, ws, state, false)
    sendResumeOffset(socket, ws)

    // The dial succeeded, so stop backing off. Whether the *session* resumed is
    // not known yet; acceptResume says so when the peer answers.
    state.reconnecting = false
    state.attempts = 0
    state.lostAt = undefined
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
