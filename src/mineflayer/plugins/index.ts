import { lastConnectOptions } from '../../react/AppStatusProvider'
import { patchResumableSocket } from '../resumableSocket'
import mouse from './mouse'
import packetsPatcher from './packetsPatcher'
import { localRelayServerPlugin } from './packetsRecording'
import ping from './ping'
import webFeatures from './webFeatures'

// register
webFeatures()
packetsPatcher()
// Must run before any socket exists: it patches Socket.prototype so the Duplex —
// and with it the cipher state, the splitter's partial frame, and the world
// model — survives losing the WebSocket underneath it.
patchResumableSocket()


customEvents.on('mineflayerBotCreated', () => {
  if (lastConnectOptions.value!.server) {
    bot.loadPlugin(ping)
  }
  bot.loadPlugin(mouse)
  if (!lastConnectOptions.value!.worldStateFileContents) {
    bot.loadPlugin(localRelayServerPlugin)
  }
})
