'use strict'

let mongodb = require('mongodb')
let streamUtil = require('stream-util')
let Timestamp = mongodb.Timestamp

const template = {
  h: '123456789',
  v: 2,
  ns: 'database.collection',
  op: 'i',
  o: {
    _id: '123456'
  }
}

function createOplogDocument (type, options) {
  let instance = JSON.parse(JSON.stringify(template))
  instance.ts = new Timestamp(0, Math.floor(new Date().getTime() / 1000))

  if (options === undefined) options = {}
  if (options.operation) instance.o = options.operation
  if (options.namespace) instance.ns = options.namespace
  switch (type) {
    case 'insert':
      instance.op = 'i'
      break
    case 'update':
      instance.op = 'u'
      instance.o2 = instance.o || {_id: '12345'}
      break
    case 'delete':
      instance.op = 'd'
      break
  }

  return instance
}

function createMongoCursor (docs) {
  if (!Array.isArray(docs)) throw new TypeError('createMongoCursor needs an array of documents as an argument')
  let cursor = streamUtil.fromArray(docs)
  cursor.find = () => {
    return cursor
  }
  return cursor
}

module.exports = {
  createOplogDocument,
  createMongoCursor
}
