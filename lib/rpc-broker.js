'use strict'

const { newEncoder } = require('./common')
const { internalTopics } = require('../../yaps-node/index.js')

function bind (wsServer, yapsRpcBroker) {
  wsServer.on('connection', (ws) => {
    const channel = newEncoder()
    channel.on('data', (d) => {
      ws.send(d)
    })
    channel.id = `${ws._socket.remoteAddress} ${ws._socket.remotePort}`
    ws.on('message', (chunk) => {
      let d
      try {
        d = JSON.parse(chunk.toString())
      } catch (error) {
        console.error(error)
      }
      if (!d) return
      if (d.t === internalTopics.RPC_EXECUTE) {
        yapsRpcBroker.schedule(channel, d.m.id, d.m.procedure, d.m.args, d.m.affinity, d.m.withStatus)
      }
    })
    ws.on('close', () => {
      console.log('close')
      channel.destroy()
    })
  })

  wsServer.on('error', (error) => {
    console.error(error)
  })

  return {
    destroy: () => {
      wsServer.shutDown()
    }
  }
}

module.exports = { bind }
