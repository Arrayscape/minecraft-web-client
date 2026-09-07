// The same assertions as the ring's PeekAt tests on the proxy side, against the
// container that plays the same role here.

import { describe, expect, it } from 'vitest'
import { ChunkedBytes } from './chunkedBytes'

const b = (s: string) => Buffer.from(s)

describe('ChunkedBytes', () => {
  it('reads from an offset without removing anything', () => {
    const c = new ChunkedBytes()
    c.append(b('0123456789'))

    expect(c.peekAt(0, 4)?.toString()).toBe('0123')
    expect(c.peekAt(3, 4)?.toString()).toBe('3456')
    expect(c.peekAt(6, 4)?.toString()).toBe('6789')
    expect(c.length).toBe(10)
  })

  it('spans chunk boundaries, which the caller cannot see', () => {
    // The offset addresses the stream, not the writes that happened to produce
    // it: where one write ended is nothing to do with where a peer is.
    const c = new ChunkedBytes()
    c.append(b('abcde'))
    c.append(b('fghij'))

    expect(c.peekAt(3, 4)?.toString()).toBe('defg')
    expect(c.peekAt(0)?.toString()).toBe('abcdefghij')
  })

  it('returns short at the end rather than padding', () => {
    const c = new ChunkedBytes()
    c.append(b('0123456789'))
    expect(c.peekAt(7, 8)?.toString()).toBe('789')
  })

  it('returns null past the end, which is not an error', () => {
    // What a consumer that has caught up sees on every call.
    const c = new ChunkedBytes()
    c.append(b('0123456789'))

    for (const off of [10, 11, 1000]) expect(c.peekAt(off, 8)).toBeNull()
    expect(c.peekAt(0, 0)).toBeNull()
    expect(c.peekAt(-1, 8)).toBeNull()
    expect(c.length).toBe(10)
  })

  it('releases from the front, including part of a chunk', () => {
    const c = new ChunkedBytes()
    c.append(b('abcde'))
    c.append(b('fghij'))

    c.release(3)
    expect(c.length).toBe(7)
    expect(c.peekAt(0)?.toString()).toBe('defghij')

    c.release(4) // crosses into the second chunk
    expect(c.peekAt(0)?.toString()).toBe('hij')

    c.release(999) // clamped
    expect(c.length).toBe(0)
    expect(c.peekAt(0)).toBeNull()
  })

  it('does not copy when the range sits inside one chunk', () => {
    // The reason this is not a ring: the producer's Buffers are kept by
    // reference, and a read that fits in one of them is a view of it.
    const c = new ChunkedBytes()
    const src = b('abcdefghij')
    c.append(src)

    const out = c.peekAt(2, 4)!
    expect(out.toString()).toBe('cdef')
    expect(out.buffer).toBe(src.buffer) // same memory, not a copy
  })
})
