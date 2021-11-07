'use strict'

const { Transform } = require('stream')

function newEncoder () {
  const result = new Transform({ objectMode: true })

  result._transform = (object, _, callback) => {
    result.push(JSON.stringify(object))
    callback()
  }
  return result
}

module.exports = { newEncoder }
