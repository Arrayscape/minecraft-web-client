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
   * Wire format is `ping:<seq>:<rxOffset>`; the proxy echoes the sequence back
   * and appends its own receive offset. It also still answers the bare
   * `ping:<seq>` form, so an older proxy stays compatible — the reply is parsed
   * on its first field either way.
   */
  bot.pingProxy = async () => {
    const ws = getProxyWs()
    if (!ws) return PING_FAILED

    const curSeq = ++seq
    const socket = bot._client.socket as any

    return new Promise<number>(resolve => {
      const sentAt = Date.now()

      const onPong = (received: string) => {
        const [ackSeq, proxyRx] = String(received).split(':')
        if (ackSeq !== curSeq.toString()) return
        // Stashed for the resume handshake, which needs the proxy's view of
        // how much of our output it accepted.
        if (proxyRx !== undefined) socket._proxyRxFromClient = Number(proxyRx)
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
        ws.send(`ping:${curSeq}:${getRxOffset()}`)
      } catch {
        // Socket closed between the readyState check and the send.
        finish(PING_FAILED)
      }
    })
  }

  let pingId = 0
  bot.pingServer = async () => {
    if (versionToNumber(bot.version) < versionToNumber('1.20.2')) return bot.player?.ping ?? -1
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
  }
}
