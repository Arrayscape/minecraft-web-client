import { lastConnectOptions } from '../../react/AppStatusProvider'
import { patchOutboundQueue } from '../socketOutboundQueue'
import mouse from './mouse'
import packetsPatcher from './packetsPatcher'
import { localRelayServerPlugin } from './packetsRecording'
import ping from './ping'
import webFeatures from './webFeatures'

// register
webFeatures()
packetsPatcher()
// Must run before any socket exists: it patches Socket.prototype so that packets
// written while the connection is down are held rather than silently discarded.
patchOutboundQueue()


customEvents.on('mineflayerBotCreated', () => {
  if (lastConnectOptions.value!.server) {
    bot.loadPlugin(ping)
  }
  bot.loadPlugin(mouse)
  if (!lastConnectOptions.value!.worldStateFileContents) {
    bot.loadPlugin(localRelayServerPlugin)
  }
})
