// Stop throwing away what the player does while the connection is down.
//
// net-browserify's Socket._write calls `this._ws.send(data)` with no readyState
// check. Per the WebSocket spec, send() on a CLOSING or CLOSED socket discards
// the payload and throws nothing — so every packet written during an outage
// vanishes silently.
//
// That matters more than it sounds, because the client does not stop playing
// when the link goes down. mineflayer's physics runs on a local timer
// (physics.js:591) and writes position/look/tick_end every tick regardless of
// connection state. The packets are already being generated; they just have
// nowhere to go.
//
// This patches the prototype in place rather than replacing the module, the same
// approach index.ts:638 already takes with handleStringMessage. Phase 3 replaces
// `net` with a resumable Socket outright, at which point this becomes part of
// that implementation and this shim goes away.
//
// Phase 2 scope: hold the bytes and account for them. Nothing replays them yet —
// there is no resume handshake to replay them into, and delivering them on a
// fresh connection would desync the cipher rather than help.

import net from 'net'

/** Cap the queue so a long outage cannot grow it without bound. */
const MAX_QUEUED_BYTES = 4 * 1024 * 1024

export interface OutboundQueueStats {
  /** Bytes accepted while the socket was not open. */
  queuedBytes: number
  /** Writes accepted while the socket was not open. */
  queuedWrites: number
  /** Bytes dropped after the cap was hit — these are genuinely lost. */
  droppedBytes: number
  /** Whether the cap has been hit on this socket. */
  overflowed: boolean
}

const statsFor = (socket: any): OutboundQueueStats => {
  socket._outboundQueueStats ??= { queuedBytes: 0, queuedWrites: 0, droppedBytes: 0, overflowed: false }
  return socket._outboundQueueStats
}

/** Queue state for a socket, for tests and diagnostics. */
export const getOutboundQueueStats = (socket: any): OutboundQueueStats | undefined => socket?._outboundQueueStats

let patched = false

export const patchOutboundQueue = () => {
  if (patched) return
  patched = true

  const { Socket } = (net as any)
  if (!Socket?.prototype?._write) return // not the browser shim; nothing to patch

  const originalWrite = Socket.prototype._write

  Socket.prototype._write = function (chunk: any, encoding: any, callback: any) {
    const ws = this._ws
    if (ws && ws.readyState === WebSocket.OPEN) {
      return originalWrite.call(this, chunk, encoding, callback)
    }

    // Socket not open. Hold the bytes instead of letting send() eat them.
    const stats = statsFor(this)
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding)

    this._outboundQueue ??= []

    if (stats.queuedBytes + buf.length > MAX_QUEUED_BYTES) {
      // Past the cap the session is unrecoverable regardless: a byte-level
      // queue cannot skip anything, so a gap here would desync the cipher on
      // any later resume. Record it so the failure is visible rather than
      // mysterious.
      stats.droppedBytes += buf.length
      if (!stats.overflowed) {
        stats.overflowed = true
        console.warn(`[outbound-queue] exceeded ${MAX_QUEUED_BYTES} bytes; this session can no longer be resumed`)
      }
    } else {
      this._outboundQueue.push(buf)
      stats.queuedBytes += buf.length
      stats.queuedWrites++
    }

    // Report success upward. The stream must keep accepting writes: applying
    // backpressure here would stall the physics loop, which is the thing
    // keeping the player in control during the outage.
    callback?.()
    return true
  }
}
