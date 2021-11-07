'use strict'

const client = require('./lib/client.js')
const server = require('./lib/server.js')

module.exports = {
  createWsClient: client.createWsClient,
  createWsServer: server.createWsServer
}
