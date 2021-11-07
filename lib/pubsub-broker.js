'use strict'

const { newEncoder } = require('./common')
const { internalTopics } = require('../../yaps-node/index.js')

function bind (wsServer, yapsPubsubBroker) {
  wsServer.on('connection', (ws) => {
    const channel = newEncoder()
    channel.on('data', (d) => ws.send(d))
    channel.id = `${ws._socket.remoteAddress} ${ws._socket.remotePort}`
    ws.on('message', (chunk) => {
      let d
      try {
        d = JSON.parse(chunk.toString())
      } catch (error) {
        console.error(error)
      }
      if (!d) return
      if (d.t === internalTopics.SUBSCRIBE) {
        yapsPubsubBroker.subscribe(channel, d.m.topic, d.m.offset)
      } else if (d.t === internalTopics.UNSUBSCRIBE) {
        yapsPubsubBroker.unsubscribe(channel, d.m.topic)
      } else {
        yapsPubsubBroker.publish(d.t, d.m)
      }
    })
    ws.on('close', () => {
      channel.destroy()
      console.log('close')
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
