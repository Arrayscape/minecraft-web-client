// The same assertions as mwcproxy/streambuffer_test.go, against the same
// component. Where the two implementations are allowed to differ — a full
// buffer — the difference is asserted rather than assumed.

import { describe, expect, it } from 'vitest'
import { StreamBuffer, StreamPositionError } from './streamBuffer'

const buf = (s: string) => Buffer.from(s)

describe('StreamBuffer', () => {
  it('does not release what it hands out', () => {
    // The property the whole design rests on: bytes given to a sender stay held
    // until they are acknowledged, so they are still there if it has to start
    // over.
    const b = new StreamBuffer(1024)
    b.write(buf('hello'))

    expect(b.next()?.toString()).toBe('hello')
    expect(b.length).toBe(5)
    expect(b.next()).toBeNull() // the cursor moved; nothing new
    expect(b.acked).toBe(0)

    b.resync(0)
    expect(b.next()?.toString()).toBe('hello')
  })

  it('keeps the cursor ahead of the head', () => {
    // The two are separate because acknowledgements lag sending. A sender sends
    // many times between them, and everything in between is in flight.
    const b = new StreamBuffer(1024)

    b.write(buf('0123456789'))
    b.next()
    expect(b.acked).toBe(0)
    expect(b.position).toBe(10)
    expect(b.produced).toBe(10)

    b.write(buf('abcde'))
    b.next()
    expect(b.acked).toBe(0)
    expect(b.position).toBe(15)
  })

  it('releases on an acknowledgement, and only then', () => {
    const b = new StreamBuffer(1024)
    b.write(buf('alphabeta'))
    b.next()

    b.ack(5)
    expect(b.acked).toBe(5)
    expect(b.length).toBe(4)
    expect(b.position).toBe(9) // an ack does not move the cursor

    b.resync(5)
    expect(b.next()?.toString()).toBe('beta')
  })

  it('treats a stale acknowledgement as a no-op', () => {
    // They repeat and reorder: a resuming peer states a position its heartbeat
    // may already have delivered.
    const b = new StreamBuffer(1024)
    b.write(buf('alphabeta'))
    b.next()
    b.ack(5)

    for (const stale of [0, 3, 5]) b.ack(stale)
    expect(b.length).toBe(4)
    expect(b.acked).toBe(5)
  })

  it('refuses an acknowledgement for bytes never sent', () => {
    // Releasing on such a claim would drop bytes still waiting to go, and the
    // gap would surface far away as a stream that no longer parses.
    const b = new StreamBuffer(1024)
    b.write(buf('six!!!'))

    expect(() => b.ack(1000)).toThrow(StreamPositionError)
    try {
      b.ack(1000)
    } catch (e) {
      expect((e as StreamPositionError).fault).toBe('beyond-sent')
    }
    expect(b.length).toBe(6)
  })

  it('refuses a resync onto released bytes', () => {
    const b = new StreamBuffer(1024)
    b.write(buf('alphabeta'))
    b.next()
    b.ack(5)

    try {
      b.resync(2)
      throw new Error('resync below the head was accepted')
    } catch (e) {
      expect((e as StreamPositionError).fault).toBe('released')
    }
    try {
      b.resync(50)
      throw new Error('resync beyond what was sent was accepted')
    } catch (e) {
      expect((e as StreamPositionError).fault).toBe('beyond-sent')
    }
  })

  it('runs the cursor ahead with nothing acknowledged', () => {
    // Acknowledgements free memory; they are not flow control. A sender that
    // could only advance when one arrived would move at whatever rate the peer
    // volunteered them.
    const b = new StreamBuffer(256 * 1024)
    for (let i = 0; i < 50; i++) b.write(Buffer.alloc(4 * 1024, 'x'))

    let read = 0
    for (let i = 0; i < 64; i++) {
      const out = b.next(32 * 1024)
      if (!out) break
      read += out.length
    }
    expect(read).toBe(200 * 1024)
    expect(b.length).toBe(200 * 1024) // reading released nothing
  })

  it('resyncs into the middle of a chunk', () => {
    // The peer's position has nothing to do with where this side happened to
    // split its writes.
    const b = new StreamBuffer(1024)
    b.write(buf('abcde'))
    b.write(buf('fghij'))
    b.next()

    b.resync(3)
    expect(b.next()?.toString()).toBe('defghij')
    expect(b.acked).toBe(3)
  })

  it('refuses a write it cannot hold, whole', () => {
    // Where the two implementations differ, and the only place they do. The Go
    // side blocks its producer here, which closes the TCP window and stops the
    // far end. This side cannot block — its producer is a physics loop — so it
    // refuses, and the caller ends the session.
    //
    // What it must never do is write part of it: half a chunk in the buffer is a
    // stream with a hole in it.
    const b = new StreamBuffer(16)
    expect(b.write(Buffer.alloc(10))).toBe(true)
    expect(b.write(Buffer.alloc(10))).toBe(false)
    expect(b.length).toBe(10)
    expect(b.produced).toBe(10)

    b.next()
    b.ack(10)
    expect(b.write(Buffer.alloc(10))).toBe(true)
  })
})
