import { versionToNumber } from 'renderer/viewer/common/utils'

// How often the client volunteers its receive offset to the proxy.
//
// The proxy detects a dead client on its own, via protocol-level WebSocket
// pings that browsers answer without JS involvement. This heartbeat exists for
// the other half: it is the only way the proxy learns how many bytes we have
// actually accepted, which is what its drop log reports as in-flight — and what
// the resume handshake will replay from once it exists.
const HEARTBEAT_INTERVAL = 5000

// A proxy that never answers is not an error worth surfacing, but it must not
// leave the caller hanging either. See the listener leak this replaces.
const PING_TIMEOUT = 5000

/** Resolved value of pingProxy() when the proxy did not answer in time. */
const PING_FAILED = -1

/**
 * The proxy's own WebSocket, when the transport has one.
 *
 * Direct `ws://` connections use a plain Duplex with no proxy behind it, so
 * there is nothing to ping and nothing to report an offset to.
 */
const getProxyWs = (): WebSocket | undefined => {
  const socket = bot?._client?.socket as any
  const ws = socket?._ws as WebSocket | undefined
  return ws?.readyState === WebSocket.OPEN ? ws : undefined
}

/**
 * Bytes this client has accepted from the proxy — the offset of the next byte
 * it needs.
 *
 * net-browserify increments `bytesRead` as each binary frame is pushed into the
 * stream, which is exactly the right moment: data buffered inside the Duplex
 * but not yet consumed by the decipher has still been received.
 */
const getRxOffset = (): number => {
  const socket = bot?._client?.socket as any
  return socket?.bytesRead ?? 0
}

export default () => {
  let seq = 0

  /**
   * Round-trip time to the proxy, or PING_FAILED if it did not answer.
   *
   * Wire format is `ping:<seq>:<offset>`, answered with `pong:<seq>:<offset>`.
   * Both offsets are absolute, counted since the stream began; ours says how
   * much of the proxy's stream we have received, theirs how much of ours they
   * have accepted. The offset is not optional — a ping without one carries the
   * only part worth having, and the proxy ignores it.
   */
  bot.pingProxy = async () => {
    const ws = getProxyWs()
    if (!ws) return PING_FAILED

    const curSeq = ++seq
    const socket = bot._client.socket as any

    return new Promise<number>(resolve => {
      const sentAt = Date.now()

      const onPong = (received: string) => {
        const [ackSeq] = String(received).split(':')
        // Prefixed to keep this sender's sequence apart from resumableSocket's
        // window prompts, which share the socket and once used the same numbers.
        // Identity matters here and only here: the offset a pong carries is the
        // proxy's session-wide receive count, true whoever asked for it, and
        // resumableSocket applies every one it sees. A round trip is the one
        // thing that is meaningless unless it is answering *your* ping.
        if (ackSeq !== `p${curSeq}`) return
        finish(Date.now() - sentAt)
      }

      // Declared before the timer so that `finish` can clear it, and the timer
      // before any path that calls `finish` — the listener is registered below.
      const finish = (result: number) => {
        clearTimeout(timer)
        socket.off('pong', onPong)
        resolve(result)
      }
      const timer = setTimeout(() => finish(PING_FAILED), PING_TIMEOUT)

      socket.on('pong', onPong)

      try {
        ws.send(`ping:p${curSeq}:${getRxOffset()}`)
      } catch {
        // Socket closed between the readyState check and the send.
        finish(PING_FAILED)
      }
    })
  }

  let pingId = 0
  bot.pingServer = async () => {
    // Below 1.20.2 there is no ping_request packet, so there is no way to time a
    // round trip from here. What comes back instead is the server's own
    // player-list figure: coarse, delayed, and measured by someone else. It is
    // reported so callers can say so rather than passing it off as a round trip
    // — it can and does read lower than legs we timed ourselves, which is how a
    // "total" ended up below one of its own parts on screen.
    if (versionToNumber(bot.version) < versionToNumber('1.20.2')) {
      bot.pingServerIsReported = true
      return bot.player?.ping ?? -1
    }
    bot.pingServerIsReported = false
    return new Promise<number>((resolve) => {
      const curId = pingId++
      bot._client.write('ping_request', { id: BigInt(curId) })
      const date = Date.now()
      const onPong = (data: { id: bigint }) => {
        if (BigInt(data.id) !== BigInt(curId)) return
        bot._client.off('ping_response' as any, onPong)
        resolve(Date.now() - date)
      }
      bot._client.on('ping_response' as any, onPong)
    })
  }

  // Steady heartbeat, independent of whether anything is displaying a ping.
  // Fire-and-forget: no listener is registered, so an unanswered heartbeat
  // costs nothing.
  const heartbeat = setInterval(() => {
    const ws = getProxyWs()
    if (!ws) return
    try {
      ws.send(`ping:0:${getRxOffset()}`)
    } catch {}
  }, HEARTBEAT_INTERVAL)

  bot.on('end', () => {
    clearInterval(heartbeat)
  })
}

declare module 'mineflayer' {
  interface Bot {
    pingProxy: () => Promise<number>
    pingServer: () => Promise<number | undefined>
    /** True when pingServer returned the server's own figure rather than a round trip we timed. */
    pingServerIsReported?: boolean
  }
}
