// A retransmission window: the client half of the same component the proxy runs
// as `mwcproxy/streambuffer.go`, with the same positions, the same operations
// and the same refusals.
//
// Three positions describe it, all counted in bytes since the stream began:
//
//   head      the oldest byte still held. Moves only on ack.
//   cursor    the next byte next() returns. Moves on next(), and on resync().
//   produced  everything written. head + length.
//
//   head <= cursor <= produced
//
// head and cursor are separate because acknowledgements lag sending by whole
// seconds, so a sender sends many times between them. Everything between the two
// is in flight: handed out, unconfirmed, and retained because it is exactly what
// a resume replays.
//
// The one thing that differs from the Go implementation is what a full buffer
// does, and it is a policy at the producer boundary rather than anything below
// it. There, the producer blocks: a pump goroutine stalls, the TCP window closes,
// and the far end stops sending. Here the producer is mineflayer's physics loop,
// which cannot be blocked — stalling it freezes the player, which is the thing
// resumption exists to prevent — so `write` refuses instead and the caller ends
// the session. Full means backpressure on one side and death on the other;
// everything downstream of `write` is identical.

import { ChunkedBytes } from './chunkedBytes'

/** Why a stated position was refused. Mirrors the proxy's two sentinels. */
export type StreamPositionFault =
  /** The peer claims bytes that were never sent to it. */
  | 'beyond-sent'
  /** The peer asks to resume from bytes already released. */
  | 'released'

export class StreamPositionError extends Error {
  constructor (readonly fault: StreamPositionFault, message: string) {
    super(message)
    this.name = 'StreamPositionError'
  }
}

export class StreamBuffer {
  /** The bytes themselves. This type owns only the three positions into them. */
  private readonly bytes = new ChunkedBytes()
  private headOffset = 0
  private cursorOffset = 0

  constructor (readonly capacity: number) {}

  /** The oldest byte still held: everything below it is confirmed and released. */
  get acked () { return this.headOffset }

  /** The offset of the next byte `next` will return. */
  get position () { return this.cursorOffset }

  /** Everything ever written to the stream. */
  get produced () { return this.headOffset + this.bytes.length }

  /** How many bytes are held — written, not yet released. */
  get length () { return this.bytes.length }

  /** How many bytes are waiting at the cursor, written but not yet taken. */
  get unsent () { return this.produced - this.cursorOffset }

  /**
   * Append to the stream. Returns false if it would not fit, having written
   * nothing.
   *
   * A partial write is not on offer: half a chunk in the buffer is a stream with
   * a hole in it, which is the failure this whole design exists to avoid.
   */
  write (buf: Buffer): boolean {
    if (buf.length === 0) return true
    if (this.bytes.length + buf.length > this.capacity) return false

    this.bytes.append(buf)
    return true
  }

  /**
   * Take up to `max` bytes from the cursor and advance it. Returns null when
   * there is nothing there yet.
   *
   * Releases nothing: what it returns stays held until it is acknowledged,
   * because until then it is what a resync would have to replay.
   */
  next (max = Number.MAX_SAFE_INTEGER): Buffer | null {
    // Read at the cursor, not at the head. The two differ by whatever is in
    // flight — handed out, not yet acknowledged — and the store is asked for
    // the offset directly, exactly as the proxy asks its ring.
    const out = this.bytes.peekAt(this.cursorOffset - this.headOffset, max)
    if (!out) return null

    this.cursorOffset += out.length
    return out
  }

  /**
   * Release everything up to `offset`: the peer has it, so it need not be kept.
   *
   * Absolute, which makes a repeated or reordered acknowledgement a no-op rather
   * than a problem. Does not move the cursor — what the peer has received says
   * nothing about what should be sent next.
   */
  ack (offset: number) {
    if (offset <= this.headOffset) return // stale or repeated: ordinary
    if (offset > this.cursorOffset) {
      throw new StreamPositionError('beyond-sent',
        `peer acknowledged ${offset}, but only ${this.cursorOffset} was ever sent`)
    }
    this.release(offset)
  }

  /**
   * Release everything up to `offset` and send the cursor back to it, so the
   * next `next()` returns the byte at `offset`.
   *
   * This is what a returning peer states: I have this much of your stream, and I
   * need everything after it. One operation because it is one fact — the peer's
   * position — and splitting it would leave an order in which the two halves
   * could be applied wrongly.
   */
  resync (offset: number) {
    if (offset < this.headOffset) {
      throw new StreamPositionError('released',
        `peer asked to resume from ${offset}, released through ${this.headOffset}`)
    }
    if (offset > this.cursorOffset) {
      throw new StreamPositionError('beyond-sent',
        `peer asked to resume from ${offset}, but only ${this.cursorOffset} was ever sent`)
    }
    this.release(offset)
    this.cursorOffset = offset
  }

  /** Drop retained bytes up to an absolute offset. Callers check the bounds. */
  private release (offset: number) {
    this.bytes.release(offset - this.headOffset)
    this.headOffset = offset
  }
}
