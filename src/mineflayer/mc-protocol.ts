import net from 'net'
import { Client } from 'minecraft-protocol'
import { versionsByMinecraftVersion, postNettyVersionsByProtocolVersion } from 'minecraft-data'
import { appQueryParams } from '../appParams'
import { downloadAllMinecraftData, getVersionAutoSelect } from '../connect'
import { gameAdditionalState } from '../globalState'
import { ProgressReporter } from '../core/progressReporter'
import { parseServerAddress } from '../parseServerAddress'
import { getCurrentProxy } from '../react/ServersList'
import { pingServerVersion, validatePacket } from './minecraft-protocol-extra'
import { UserError } from './userError'
import { getWebsocketStream } from './websocket-core'

let lastPacketTime = 0
customEvents.on('mineflayerBotCreated', () => {
  // const oldParsePacketBuffer = bot._client.deserializer.parsePacketBuffer
  //   try {
  //     const parsed = oldParsePacketBuffer(buffer)
  //   } catch (err) {
  //     debugger
  //     reportError(new Error(`Error parsing packet ${buffer.subarray(0, 30).toString('hex')}`, { cause: err }))
  //     throw err
  //   }
  // }
  class MinecraftProtocolError extends Error {
    constructor (message: string, cause?: Error, public data?: any) {
      if (data?.customPayload) {
        message += ` (Custom payload: ${data.customPayload.channel})`
      }
      super(message, { cause })
      this.name = 'MinecraftProtocolError'
    }
  }

  const onClientError = (err, data) => {
    const error = new MinecraftProtocolError(`Minecraft protocol client error: ${err.message}`, err, data)
    reportError(error)
  }
  if (typeof bot._client['_events'].error === 'function') {
    // dont report to bot for more explicit error
    bot._client['_events'].error = onClientError
  } else {
    bot._client.on('error' as any, onClientError)
  }

  // todo move more code here
  if (!appQueryParams.noPacketsValidation) {
    (bot._client as unknown as Client).on('packet', (data, packetMeta, buffer, fullBuffer) => {
      validatePacket(packetMeta.name, data, fullBuffer, true)
      lastPacketTime = performance.now()
    });
    (bot._client as unknown as Client).on('writePacket', (name, params) => {
      validatePacket(name, params, Buffer.alloc(0), false)
    })
  }
})

setInterval(() => {
  if (!bot || !lastPacketTime) return
  if (bot.player?.ping > 500) { // TODO: we cant rely on server ping 1. weird calculations 2. available with delays instead patch minecraft-protocol to get latency of keep_alive packet
    gameAdditionalState.poorConnection = true
  } else {
    gameAdditionalState.poorConnection = false
  }
  if (performance.now() - lastPacketTime < 2000) {
    gameAdditionalState.noConnection = false
    return
  }
  gameAdditionalState.noConnection = true
}, 1000)


export const getServerInfo = async (ip: string, port?: number, preferredVersion = getVersionAutoSelect(), ping = false, progressReporter?: ProgressReporter, setProxyParams?: ProxyParams) => {
  await downloadAllMinecraftData()
  const isWebSocket = ip.startsWith('ws://') || ip.startsWith('wss://')
  let stream
  if (isWebSocket) {
    progressReporter?.setMessage('Connecting to WebSocket server')
    stream = (await getWebsocketStream(ip)).mineflayerStream
    progressReporter?.setMessage('WebSocket connected. Ping packet sent, waiting for response')
  } else if (setProxyParams) {
    setProxy(setProxyParams)
  }
  window.setLoadingMessage = (message?: string) => {
    if (message === undefined) {
      progressReporter?.endStage('dns')
    } else {
      progressReporter?.beginStage('dns', message)
    }
  }
  return pingServerVersion(ip, port, {
    ...(stream ? { stream } : {}),
    ...(ping ? { noPongTimeout: 3000 } : {}),
    ...(preferredVersion ? { version: preferredVersion } : {}),
  }).finally(() => {
    window.setLoadingMessage = undefined
  })
}

// Magic-link mode: the proxy runs the status ping itself and answers for the
// servers its session's batch names — the proxy only lets a session open one
// TCP connection to log in on, and a status ping can't be followed by a login
// on the same connection.
//
// The preferred version's protocol goes in the ping's Handshake, as
// minecraft-protocol's own ping does: a multi-version server (Velocity,
// ViaVersion) answers with it when it can serve it, and with its own newest
// otherwise. The version is then taken from the reply's protocol number only —
// a branded name like "Velocity 1.7.2-26.2" carries the software's version,
// not Minecraft's, so guessing from it picks nonsense.
export const getServerInfoViaProxy = async (proxyBaseUrl: string, magicCode: string, host: string, port = 25_565, preferredVersion = getVersionAutoSelect()) => {
  await downloadAllMinecraftData()
  if (!proxyBaseUrl.startsWith('http')) proxyBaseUrl = `${location.protocol === 'http:' ? 'http' : 'https'}://${proxyBaseUrl}`
  const preferredProtocol = preferredVersion ? versionsByMinecraftVersion.pc[preferredVersion]?.version : undefined
  const resp = await fetch(`${proxyBaseUrl}/api/vm/net/ping`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ magicCode, protocol: preferredProtocol }),
  })
  if (!resp.ok) throw new Error(`Cannot ping server [${resp.status}]: ${await resp.text()}`)
  const { servers } = await resp.json() as { servers: Array<{ host: string, port: number, status?: any, error?: string }> }
  const entry = servers.find(s => s.host.toLowerCase() === host.toLowerCase() && s.port === port)
  if (!entry) throw new Error(`${host}:${port} is not one of this magic link's servers`)
  if (!entry.status) throw new Error(`Server ${host}:${port}: ${entry.error ?? 'no response'}`)

  const { name, protocol } = entry.status.version ?? {}
  // The typings say one version per protocol; at runtime it's a list.
  const byProtocol = postNettyVersionsByProtocolVersion.pc as unknown as Record<number, Array<{ minecraftVersion: string }>>
  const known = byProtocol[protocol]
  if (!known?.length) {
    throw new UserError(`Server ${host}:${port} runs ${name ?? 'an unknown version'} (protocol ${protocol}), which this client doesn't support`)
  }
  return { version: known[0].minecraftVersion, latency: 0, fullInfo: entry.status }
}

globalThis.debugTestPing = async (ip: string) => {
  const parsed = parseServerAddress(ip, false)
  const result = await getServerInfo(parsed.host, parsed.port ? Number(parsed.port) : undefined, undefined, true, undefined, { address: getCurrentProxy(), })
  console.log('result', result)
  return result
}

export const getDefaultProxyParams = () => {
  return {
    headers: {
      Authorization: `Bearer ${new URLSearchParams(location.search).get('token') ?? ''}`
    }
  }
}

export type ProxyParams = {
  address?: string
  headers?: Record<string, string>
}

export const setProxy = (proxyParams: ProxyParams) => {
  if (proxyParams.address?.startsWith(':')) {
    proxyParams.address = `${location.protocol}//${location.hostname}${proxyParams.address}`
  }
  if (proxyParams.address && location.port !== '80' && location.port !== '443' && !/:\d+$/.test(proxyParams.address)) {
    const https = proxyParams.address.startsWith('https://') || location.protocol === 'https:'
    proxyParams.address = `${proxyParams.address}:${https ? 443 : 80}`
  }

  const parsedProxy = parseServerAddress(proxyParams.address, false)
  const proxy = { host: parsedProxy.host, port: parsedProxy.port }
  proxyParams.headers ??= getDefaultProxyParams().headers
  net['setProxy']({
    hostname: proxy.host,
    port: proxy.port,
    headers: proxyParams.headers,
    artificialDelay: appQueryParams.addPing ? Number(appQueryParams.addPing) : undefined
  })
  return {
    proxy
  }
}
