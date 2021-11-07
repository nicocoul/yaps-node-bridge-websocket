
const yaps = require('../../yaps-node/index.js')
const { bind } = require('../lib/pubsub-broker.js')
const { createWsClient } = require('../lib/client.js')

const pubsubBroker = yaps.pubsub.createBroker({ port: 8080 })
const pubsubClient = yaps.pubsub.createClient({ host: 'localhost', port: 8080 })

for (let i = 0; i < 10; i++) {
  pubsubClient.publish('topic1', { i })
}

const { WebSocketServer } = require('ws')
const wss = new WebSocketServer({ port: 8081 })
bind(wss, pubsubBroker)

const wsClient = createWsClient('localhost', 8081)

wsClient.subscribe('topic1', (message, offset) => {
  console.log(message, offset)
})
