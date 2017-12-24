'use strict'

/**
 * An object containing the configuration options of the OplogEmitter
 * @typedef {object} OplogOptions
 * @property {string} oplogURL                      A mongodb connection string to the oplog
 * @property {?TimestampGenerator} getLastTimestamp A function returning a mongodb Timestamp with the starting offset in the oplog
 * @property {?string} database                     Filter oplog events by this database name
 * @property {?string} collection                   Filter oplog events by this collection name
 * @property {?Credentials} credentials             An object of mongodb credentials
 * @property {?number} retries                      The amount of times to retry connecting to the database (with exponential-backoff)
 * @property {?function} log                        A function which this library can use to log
 * @property {?number} timestampTimeout             The number of milliseconds we should wait for getLastTimestamp to return a result
 * @public
 */

/**
 * An object with mongodb credentials of a user in the admin database
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

let bunyan = require('bunyan')
let EventEmitter = require('events')
let poller = require('promise-poller').default
let mongodb = require('mongodb')
let MongoClient = mongodb.MongoClient
let Timestamp = mongodb.Timestamp

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
 * @throws {TypeError}               when constructor arguments are not valid
 * @public
 */
class OplogEmitter extends EventEmitter {
  constructor (args) {
    super()

    args = validateArgs(args)

    let oplog

    let pollerOptions = {
      retries: args.retries,
      strategy: 'exponential-backoff',
      taskFn: connectToMongo.bind(null, args.oplogURL, args.log, args.credentials)
    }

    poller(pollerOptions)
      .then((collection) => {
        args.log('Connected to mongodb local collection')
        oplog = collection
        return poller({
          retries: 1,
          masterTimeout: args.timestampTimeout,
          taskFn: args.getLastTimestamp
        })
      })
      .then((lastTimestamp) => {
        if (!(lastTimestamp instanceof Timestamp)) throw new Error('getLastTimestamp() should return a mongodb.Timestamp')
        const query = {
          ts: {$gt: lastTimestamp}
        }

        const options = {
          tailable: true,
          awaitdata: true,
          oplogReplay: true,
          numberOfRetries: -1
        }

        args.log('Opening tailable cursor to the oplog with timestamp: ' + lastTimestamp)
        oplog.find(query, options).stream()
          .on('data', routeEvent.bind(null, this))
          .on('end', () => {
            args.log('Tailable cursor was closed')
            this.emit('error', new Error('Database cursor closed unexpectedly'))
          })
      })
      .catch((errors) => {
        let error = Array.isArray(errors)
          ? errors[errors.length - 1]
          : errors
        if (error === 'master timeout') error = new Error('getLastTimestamp did not resolve before the timeout')
        this.emit('error', error)
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

  args.retries = args.retries === undefined ? 5 : args.retries
  if (typeof args.retries !== 'number') throw new TypeError('retries should be a number')

  args.log = args.log === undefined
    ? (message) => bunyan.createLogger({name: 'oplog-emitter', src: true}).trace(message)
    : args.log
  if (typeof args.log !== 'function') throw new TypeError('log should be a function that logs strings')

  args.timestampTimeout = args.timestampTimeout === undefined ? 30000 : args.timestampTimeout
  if (typeof args.timestampTimeout !== 'number') throw new TypeError('timestampTimeout should be a number')

  return args
}

/**
 * Establish a connection to the mongo oplog collection
 * @param  {string}       oplogURL    The mongodb connection string
 * @param  {function}     log         A function that can log strings
 * @param  {?Credentials} credentials Optional mongodb credentials
 * @return {Promise<Collection>}      A Promise resolving to the oplog collection
 * @private
 */
function connectToMongo (oplogURL, log, credentials) {
  log('Connecting to mongodb: ' + oplogURL)
  const options = credentials
    ? {
      auth: {
        user: credentials.username,
        password: credentials.password
      },
      authSource: 'adming'
    }
    : {}
  return new MongoClient(oplogURL, options)
    .connect()
    .then((client) => {
      return getOplogCollection(client.db('local'), log)
    })
}

/**
 * Return a promise to the oplog collection. Rejects if the database has no oplog
 * @param  {object}   db            A mongodb database Handle
 * @param  {function} log           A function that can log strings
 * @return {Promise<Collection>}    A promise resolving to the oplog collection
 * @private
 */
function getOplogCollection (db, log) {
  return new Promise((resolve, reject) => {
    db.collection('oplog.rs', {strict: true}, (error, oplog) => {
      if (error) {
        log(error)
        db.collection('oplog.$main', {strict: true}, (error2, mainOplog) => {
          if (error2) {
            log(error2)
            return reject(new Error('Could not find oplog collection. Make sure mongodb is configured for replication'))
          }
          resolve(mainOplog)
        })
      } else {
        resolve(oplog)
      }
    })
  })
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
