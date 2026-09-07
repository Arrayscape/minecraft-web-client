// A bounded byte store that hands out slices at an offset.
//
// The client's half of what `Arrayscape/ringbuffer` is on the proxy: the
// container underneath `StreamBuffer`, with the same job and the same
// offset-addressed read (`peekAt` ↔ `PeekAt`). `StreamBuffer` sits on this
// exactly as the Go one sits on the ring, so both ends read the same at both
// levels.
//
// It is deliberately NOT a ring, and that is not an oversight. A ring means
// copying every write into a preallocated array and holding the whole capacity
// from the start. Here the producer hands over Buffers that mineflayer has
// already allocated, so keeping the references costs nothing and the memory is
// what is actually retained. The proxy needs a ring because it absorbs a TCP
// stream into a fixed allocation to create backpressure; neither applies in a
// browser. Same interface, different structure, for reasons that belong to the
// runtime rather than to the design.

export class ChunkedBytes {
  /** Retained slices, oldest first, together forming one contiguous stream. */
  private chunks: Buffer[] = []
  private held = 0

  /** How many bytes are stored. */
  get length () { return this.held }

  /** Take a reference to `buf`. The caller must not mutate it afterwards. */
  append (buf: Buffer) {
    if (buf.length === 0) return
    this.chunks.push(buf)
    this.held += buf.length
  }

  /**
   * Up to `max` bytes starting `off` bytes in, without removing anything.
   *
   * Returns null when there is nothing at that offset — which for a consumer
   * that has caught up is the ordinary answer, not an error, and is the same
   * distinction the Go side draws.
   *
   * Nothing is copied when the range sits inside one chunk, which is the common
   * case: the result is a view. Only a range spanning chunks is concatenated.
   */
  peekAt (off: number, max = Number.MAX_SAFE_INTEGER): Buffer | null {
    if (off < 0 || max <= 0) return null

    const available = this.held - off
    if (available <= 0) return null

    const want = Math.min(available, max)
    const out: Buffer[] = []
    let skip = off
    let taken = 0

    for (const chunk of this.chunks) {
      if (skip >= chunk.length) {
        skip -= chunk.length
        continue
      }
      const slice = chunk.subarray(skip, Math.min(chunk.length, skip + (want - taken)))
      skip = 0
      out.push(slice)
      taken += slice.length
      if (taken === want) break
    }

    return out.length === 1 ? out[0] : Buffer.concat(out)
  }

  /** Drop the first `n` bytes. Clamped to what is stored. */
  release (n: number) {
    let drop = Math.min(n, this.held)

    while (drop > 0 && this.chunks.length > 0) {
      const head = this.chunks[0]
      if (head.length <= drop) {
        drop -= head.length
        this.held -= head.length
        this.chunks.shift()
      } else {
        this.chunks[0] = head.subarray(drop)
        this.held -= drop
        drop = 0
      }
    }
  }
}
