const yaps = require('yaps-node')

const TOPICS = {
    SUBSCRIBER: '_int_subscriber',
    PUBLISHER: '_int_publisher',
    RPC_EXECUTE: '_int_rpc_execute'
}

function newTopicSubscribers() {
    const state = []
    const getIndex = (topic, clientId) => {
        return state.findIndex(s => s.topic === topic && s.clientId === clientId)
    }
    return {
        exists: (topic, clientId) => {
            return getIndex(topic, clientId) !== -1
        },
        add: (topic, clientId, sourceStream) => {
            if (getIndex(topic, clientId) !== -1) return
            state.push({ topic, clientId, sourceStream })
        },
        remove: (topic, clientId) => {
            const index = getIndex(topic, clientId)
            if (index !== -1) {
                state[index].sourceStream.destroy()
                state.splice(index, 1)
            }
        },
        removeClient: (clientId) => {

        },
        clientIdsByTopic: (topic) => {
            return state.filter(s => s.topic === topic).map(s => s.id)
        }
    }
}

async function createWsServer(httpServer, brokerHost, brokerPort) {
    const pubSubClient = yaps.createPubSubClient({ host: brokerHost, port: brokerPort })
    const rcpClient = yaps.createRpcClient({ host: brokerHost, port: brokerPort })
    const subscribers = newTopicSubscribers()
    const wsServer = new WsServer({
        httpServer: httpServer,
        autoAcceptConnections: false
    })

    wsServer.on('request', (r) => {
        const connection = r.accept('yaps', request.origin)

        connection.on('message', async (message) => {
            if (message.type !== 'utf8') {
                return
            }
            let r = ''
            try {
                r = JSON.parse(message.utf8Data)
            } catch (error) {
                return
            }
            if (!r.t || !r.m) {
                return
            }
            if (r.t === TOPICS.SUBSCRIBER) {
                const topic = r.m.topic
                const fromOffset = r.m.offset

                if (!subscribers.exists(topic, c.id)) {
                    // logger.debug(`topic ${topic}: requiredOffset=${fromOffset}, cache.firstReadableOffset=${cache.get(topic).firstReadableOffset()} cache.count=${cache.get(topic).count()}, store.count=${store.get(topic).count()}`)
                    subscribers.add(topic, c.id, rs)
                } else {
                    logger.debug(`already subscribed ${JSON.stringify(d.m)}`)
                    subscribers.remove(topic, c.id)
                    subscribers.add(topic, c.id, rs)
                }
                pubSubClient.subscribe(r.m.topic, console.log)
                return
            } else if (r.t === TOPICS.RPC_EXECUTE) {
                rcpClient.execute(r.m.procedure, r.m.args, console.log, r.m.affinity)
                return
            } else {
                pubSubClient.publish(r.t, r.m)
            }
        })

        connection.on('close', function (reasonCode, description) {
            logger.info(`Peer  ${connection.remoteAddress}  disconnected: ${reasonCode} ${description}`)
        })
    })

    wsServer.on('error', function (error) {
        logger.error(error)
    })
}

module.exports = { createWsServer }
