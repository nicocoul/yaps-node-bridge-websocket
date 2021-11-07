'use strict'

const WebSocketClient = require('websocket').client
const { internalTopics } = require('../../yaps-node/index.js')

function newTopics () {
  const state = {}
  return {
    set: (topic) => {
      if (!state[topic]) { state[topic] = { offset: -1, callbacks: [] } }
    },
    setOffset: (topic, offset) => {
      state[topic].offset = offset
    },
    getOffset: (topic) => {
      return state[topic].offset
    },
    addCallback: (topic, callback) => {
      state[topic].callbacks.push(callback)
    },
    getCallbacks: (topic) => {
      return state[topic].callbacks
    },
    exists: (topic) => {
      return state[topic] !== undefined
    },
    remove: (topic) => {
      state[topic].callbacks.length = 0
      delete state[topic]
    },
    all: () => {
      return Object.keys(state)
    }
  }
}

function newQueue () {
  const state = []
  return {
    enqueue: (element) => {
      return state.push(element)
    },
    dequeue: () => {
      return state.shift()
    },
    peek: () => {
      if (state.length) {
        return state[0]
      }
    }
  }
}

function createWsClient (host, port, ssl = false) {
  const client = new WebSocketClient()
  const topics = newTopics()
  const messageQueue = newQueue()
  let connectTimeout

  const onMessage = (topic, offset, message) => {
    topics.getCallbacks(topic).forEach(callback => {
      callback(message, offset)
    })
  }

  const onClose = () => {
    connect()
  }

  const onError = (error) => {
    console.error(error)
    connect()
  }

  const onOpen = () => {
    processOutboundQueue()
  }

  const connect = () => {
    const address = `${ssl ? 'wss' : 'ws'}://${host}:${port}/`
    if (connectTimeout) {
      clearTimeout(connectTimeout)
      connectTimeout = setTimeout(() => {
        client.connect(address, 'yaps')
      }, 1000)
    } else {
      connectTimeout = true
      client.connect(address, 'yaps')
    }
  }

  const subscribe = (topic, callback) => {
    topics.set(topic)
    topics.addCallback(topic, callback)
    messageQueue.enqueue({ t: internalTopics.SUBSCRIBE, m: { topic, offset: topics.getOffset(topic) + 1 } })
    processOutboundQueue()
  }

  const unsubscribe = (topic) => {
    topics.remove(topic)
    messageQueue.enqueue({ t: internalTopics.UNSUBSCRIBE, m: { topic } })
    processOutboundQueue()
  }

  const publish = (topic, message) => {
    messageQueue.enqueue({ t: topic, m: message })
    processOutboundQueue()
  }

  const execute = (id, procedure, args, affinity, withStatus) => {
    messageQueue.enqueue({ t: internalTopics.RPC_EXECUTE, m: { id, procedure, args, affinity, withStatus } })
    processOutboundQueue()
  }

  /// ////////// SPECIFIC ////////////////
  let _connection
  const processOutboundQueue = () => {
    if (!_connection || _connection.state !== 'open') {
      return
    }
    const d = messageQueue.dequeue()
    if (d) {
      _connection.sendUTF(JSON.stringify(d))
      processOutboundQueue()
    }
  }

  client.on('connectFailed', (error) => {
    onError(error)
  })

  client.on('connect', (connection) => {
    _connection = connection
    onOpen()
    connection.on('error', (error) => {
      onError(error)
    })
    connection.on('close', () => {
      onClose()
    })
    connection.on('message', (rawData) => {
      if (rawData.type !== 'utf8') {
        return
      }
      let d
      try {
        d = JSON.parse(rawData.utf8Data)
      } catch (error) {
        return
      }
      if (!d.t || !d.m) {
        return
      }
      onMessage(d.t, d.o, d.m)
    })
  })

  connect()

  return {
    publish,
    subscribe,
    unsubscribe,
    execute
  }
}

module.exports = { createWsClient }
