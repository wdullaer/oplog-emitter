'use strict'

/**
 * An object containing the configuration options of the OplogEmitter
 * @typedef {object} OplogOptions
 * @property {string} oplogURL
 * @property {?TimestampGenerator} getLastTimestamp
 * @property {?string} database
 * @property {?string} collection
 * @property {?Credentials} credentials
 * @public
 */

/**
 * An object with mongodb credentials
 * @typedef {object} Credentials
 * @property {string} username
 * @property {string} password
 * @public
 */

/**
 * A function returning a promise to a mongodb Timestamp
 * @typedef {function} TimestampGenerator
 * @return {Promise<Timestamp>} A promise resolving to a mongodb timestamp
 * @public
 */

let EventEmitter = require('events')
let poller = require('promise-poller').default
let mongodb = require('mongodb')
let MongoClient = mongodb.MongoClient
let Timestamp = mongodb.Timestamp

let pollerOptions = {
  strategy: 'exponential-backoff',
  retries: 10
}

/**
 * An event emitter that fires for every oplog entry
 * @example
 * let OplogEmitter = require('oplog-emitter');
 * let emitter = new OplogEmitter('mongodb://localhost:27017/local?authSource=admin');
 *
 * emitter.on('op', () => console.log('A transaction was added to the oplog'));
 * emitter.on('insert', () => console.log('An insert was done in the database'));
 * emitter.on('delete', () => console.log('A document was deleted in the database'));
 * emitter.on('update', () => console.log('A document was updated in the database'));
 * emitter.on('error', () => console.log('Something went wrong when reading'));
 *
 * @class OplogEmitter
 * @param {string|OplogOptions} args constructor options
 * @fires OplogEmitter#op
 * @fires OplogEmitter#insert
 * @fires OplogEmitter#delete
 * @fires OplogEmitter#update
 * @fires EventEmitter#error
 * @throws TypeError                 when constructor arguments are not valid
 * @public
 */
class OplogEmitter extends EventEmitter {
  constructor (args) {
    super()

    args = validateArgs(args)

    let oplog

    pollerOptions.taskFn = connectToMongo.bind(null, args.oplogURL, args.credentials)

    poller(pollerOptions)
      .then((collection) => {
        oplog = collection
        return args.getLastTimestamp()
      })
      .then((lastTimestamp) => {
        const query = {
          ts: {$gt: lastTimestamp}
        }

        const options = {
          tailable: true,
          awaitdata: true,
          oplogReplay: true,
          numberOfRetries: -1
        }

        oplog.find(query, options).on('data', routeEvent.bind(null, this))
      })
      .catch((errors) => {
        this.emit('error', errors[errors.length - 1])
      })

    /**
     * Emit the proper high level events based on the oplog event
     * @param  {EventEmitter} emitter    An instance of this class
     * @param  {object}       oplogEvent A json object returned by the mongodb cursor
     * @return {undefined}               Returns void
     * @private
     */
    function routeEvent (emitter, oplogEvent) {
      if (!oplogEvent.ns.match(`${args.database}\\.${args.collection}`)) return

      emitter.emit('op', oplogEvent)
      switch (oplogEvent.op) {
        case 'i':
          emitter.emit('insert', oplogEvent)
          break
        case 'd':
          emitter.emit('delete', oplogEvent)
          break
        case 'u':
          emitter.emit('update', oplogEvent)
      }
    }
  }
}

/**
 * Validate the constructor arguments and supply defaults where necessary
 * @param  {OplogOptions|string} args The options as supplied by the user
 * @return {OplogOptions}             The options with the appropriate default values for optional attributes
 * @throws {TypeError}                Properties of args must have their specified types
 * @private
 */
function validateArgs (args) {
  if (typeof args === 'string') args = {oplogURL: args}
  if (typeof args !== 'object') throw new TypeError('argument should be a connectionstring or a map of options')

  if (args.oplogURL === undefined) throw new TypeError('oplogURL must be specified')
  if (typeof args.oplogURL !== 'string') throw new TypeError('oplogURL must be a string')

  args.getLastTimestamp = args.getLastTimestamp === undefined ? getLastTimestamp : args.getLastTimestamp
  if (typeof args.getLastTimestamp !== 'function') throw new TypeError('getLastTimestamp should be a function that returns a Promise to a Mongo Timestamp')

  args.database = args.database === undefined ? '.*' : args.database
  if (typeof args.database !== 'string') throw new TypeError('database should be a string')

  args.collection = args.collection === undefined ? '.*' : args.collection
  if (typeof args.collection !== 'string') throw new TypeError('collection should be a string')

  if (args.credentials !== undefined) {
    if (typeof args.credentials !== 'object') throw new TypeError('credentials should be provided as an object')
    if (!args.credentials.username) throw new TypeError('credentials should have an attribute username that is a string')
    if (typeof args.credentials.username !== 'string') throw new TypeError('credentials should have an attribute username that is a string')
    if (!args.credentials.password) throw new TypeError('credentials should have an attribute password that is a string')
    if (typeof args.credentials.password !== 'string') throw new TypeError('credentials should have an attribute password that is a string')
  }

  return args
}

/**
 * Establish a connection to the mongo oplog collection
 * @param  {string}       oplogURL    The mongodb connection string
 * @param  {?Credentials} credentials Optional mongodb credentials
 * @return {Promise<Collection>}      A Promise resolving to the oplog collection
 * @private
 */
function connectToMongo (oplogURL, credentials) {
  let db
  return MongoClient.connect(oplogURL)
    .then((database) => {
      db = database
      if (credentials) return db.authenticate(credentials.username, credentials.password)
      return Promise.resolve()
    })
    .then(() => db.collection('oplog.rs'))
}

/**
 * Default implementation of getLastTimestamp
 * @return {Promise<Timestamp>} A Promise resolving to a Mongo Timestamp at the current system time
 * @private
 */
function getLastTimestamp () {
  return Promise.resolve(new Timestamp(0, Math.floor(new Date().getTime() / 1000)))
}

module.exports = OplogEmitter
