'use strict'

let expect = require('chai').expect
let rewire = require('rewire')
let testUtils = require('./test-utils.js')

let createMongoCursor = testUtils.createMongoCursor
let createOplogDocument = testUtils.createOplogDocument
let EventEmitter = require('events')
let Timestamp = require('mongodb').Timestamp

let testModule = rewire('../lib')
let OplogEmitter = testModule.__get__('OplogEmitter')
let validateArgs = testModule.__get__('validateArgs')
let getLastTimestamp = testModule.__get__('getLastTimestamp')
let getOplogCollection = testModule.__get__('getOplogCollection')
let connectToMongo = testModule.__get__('connectToMongo')

function spy (func, done) {
  return function () {
    try {
      func.apply(func, arguments)
      done()
    } catch (error) {
      done(error)
    }
  }
}

describe('OplogEmitter', () => {
  let restore
  beforeEach(() => {
    restore = () => {}
  })

  afterEach(() => {
    restore()
  })

  it('should return a new instance of OplogEmittter', () => {
    expect(new OplogEmitter('test')).to.be.an.instanceOf(OplogEmitter).which.is.an.instanceOf(EventEmitter)
  })

  it('should throw an error when the arguments are not valid', () => {
    // See validateArgs tests for exhaustive testing
    // this test ensures that function gets called in the constructor
    const testFn = () => (new OplogEmitter())

    expect(testFn).to.throw(TypeError)
  })

  it('should emit an error if connection to mongodb failed', (done) => {
    const dummyConnect = () => {
      return Promise.reject(new Error('test-error'))
    }
    restore = testModule.__set__({
      connectToMongo: dummyConnect
    })

    function errorCallback (error) {
      expect(error).to.be.an('error')
    }

    let emitter = new OplogEmitter({
      oplogURL: 'test',
      retries: 1
    })
    emitter.on('error', spy(errorCallback, done))
  })

  it('should emit "insert" when an insert happens', (done) => {
    const doc = createOplogDocument('insert')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('insert', spy(callback, done))
    emitter.on('error', failCallback.bind('error'))
    emitter.on('update', failCallback.bind('update'))
    emitter.on('delete', failCallback.bind('delete'))
  })

  it('should emit "update" when an update happens', (done) => {
    const doc = createOplogDocument('update')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('update', spy(callback, done))
    emitter.on('error', failCallback.bind('error'))
    emitter.on('insert', failCallback.bind('insert'))
    emitter.on('delete', failCallback.bind('delete'))
  })

  it('should emit "delete" when a delete happens', (done) => {
    const doc = createOplogDocument('delete')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('delete', spy(callback, done))
    emitter.on('error', failCallback.bind('error'))
    emitter.on('insert', failCallback.bind('insert'))
    emitter.on('update', failCallback.bind('update'))
  })

  it('should emit "op" for an insert event', (done) => {
    const doc = createOplogDocument('insert')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('op', spy(callback, done))
    emitter.on('error', failCallback)
  })

  it('should emit "op" for an update event', (done) => {
    const doc = createOplogDocument('update')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('op', spy(callback, done))
    emitter.on('error', failCallback.bind('error'))
  })

  it('should emit "op" for a delete event', (done) => {
    const doc = createOplogDocument('delete')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('op', spy(callback, done))
    emitter.on('error', failCallback.bind('error'))
  })

  it('should emit all events', (done) => {
    const docs = [createOplogDocument('insert'), createOplogDocument('delete'), createOplogDocument('update')]
    const cursor = createMongoCursor(docs)
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    let counter = 0
    function callback (op) {
      try {
        expect(op).to.deep.equal(docs[counter])
      } catch (error) {
        done(error)
      }
      counter++
      if (counter === 3) done()
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('op', callback)
    emitter.on('error', failCallback.bind('error'))
  })

  it('should ignore events not matching the namespace', (done) => {
    const database = 'consumers'
    const collection = 'records'
    const namespace = database + '.' + collection
    const options = {namespace}
    const docs = [createOplogDocument('insert'), createOplogDocument('delete', options), createOplogDocument('update', options)]
    const cursor = createMongoCursor(docs)
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = testModule.__set__({
      connectToMongo: connect
    })

    let counter = 0
    function callback (op) {
      counter++
      try {
        expect(op).to.deep.equal(docs[counter])
      } catch (error) {
        done(error)
      }
      if (counter === 2) done()
    }
    function failCallback (error) {
      if (error === undefined) error = new Error(this + ' callback should not fire')
      done(error)
    }

    let emitter = new OplogEmitter({
      oplogURL: 'test',
      database,
      collection
    })
    emitter.on('op', callback)
    emitter.on('error', failCallback.bind('error'))
  })
})

describe('getLastTimestamp()', () => {
  it('should return a Promise', () => {
    expect(getLastTimestamp()).to.be.an.instanceOf(Promise)
  })

  it('should resolve to a Mongodb Timestamp', () => {
    return expect(getLastTimestamp()).to.eventually.be.an.instanceOf(Timestamp)
  })
})

describe('getOplogCollection', () => {
  it('should return a Promise resolving to the oplog, if local.oplog.rs exists', () => {
    const collection = 'collection'
    const db = {
      collection: (name, opts, callback) => callback(null, collection)
    }

    return expect(getOplogCollection(db)).to.eventually.equal(collection)
  })

  it('should return a Promise resolving to the oplog, if local.oplog.$main exists', () => {
    const collection = 'collection'
    const db = {
      collection: (name, opts, callback) => {
        if (name === 'oplog.$main') return callback(null, collection)
        return callback(new Error('No such collection'))
      }
    }

    return expect(getOplogCollection(db)).to.eventually.equal(collection)
  })

  it('should return a Promise that rejects if no oplog can be found', () => {
    const db = {
      collection: (name, opts, callback) => callback(new Error('No such collection'))
    }
    return expect(getOplogCollection(db)).to.eventually.be.rejectedWith('Could not find oplog collection. Make sure mongodb is configured for replication')
  })
})

describe('connectToMongo', () => {
  let restore
  beforeEach(() => {
    restore = () => {}
  })

  afterEach(() => {
    restore()
  })

  it('Should try to connect to mongodb with the oplogURL', () => {
    const oplogURL = 'mongodb://localhost:27017/consumers'
    const log = () => {}
    let called = false
    restore = testModule.__set__({
      'getOplogCollection': () => Promise.resolve('collection'),
      'MongoClient': {
        connect: (url) => {
          called = true
          expect(url).to.equal(oplogURL)
          return Promise.resolve('db')
        }
      }
    })

    return connectToMongo(oplogURL, log)
      .then(() => {
        expect(called).to.be.true
      })
  })

  it('Should get the oplog connection if we can connect to mongodb', () => {
    const oplogURL = 'mongodb://localhost:27017/consumers'
    const log = () => {}
    let called = false
    restore = testModule.__set__({
      'getOplogCollection': () => {
        called = true
        Promise.resolve('collection')
      },
      'MongoClient': {
        connect: (url) => Promise.resolve('db')
      }
    })

    return connectToMongo(oplogURL, log)
      .then(() => {
        expect(called).to.be.true
      })
  })

  it('Should reject if a connection to mongo cannot be established', () => {
    const oplogURL = 'test'
    const log = () => {}

    return expect(connectToMongo(oplogURL, log)).to.be.rejected
  })

  it('Should try to authenticate if credentials are passed in', () => {
    const oplogURL = 'mongodb://localhost:27017/consumers'
    const log = () => {}
    const credentials = {
      username: 'username',
      password: 'password'
    }
    let called = false
    const db = {
      authenticate: (username, password) => {
        called = true
        expect(username).to.equal(credentials.username)
        expect(password).to.equal(credentials.password)
      }
    }
    restore = testModule.__set__({
      'getOplogCollection': () => Promise.resolve('collection'),
      'MongoClient': {
        connect: () => {
          return Promise.resolve(db)
        }
      }
    })

    return connectToMongo(oplogURL, log, credentials)
      .then(() => {
        expect(called).to.be.true
      })
  })

  it('Should not authenticate if credentials are absent', () => {
    const oplogURL = 'mongodb://localhost:27017/consumers'
    const log = () => {}
    let called = false
    const db = {
      authenticate: () => {
        called = true
        throw new Error('Authenticate should not be called')
      }
    }
    restore = testModule.__set__({
      'getOplogCollection': () => Promise.resolve('collection'),
      'MongoClient': {
        connect: () => {
          return Promise.resolve(db)
        }
      }
    })

    return connectToMongo(oplogURL, log)
      .then(() => {
        expect(called).to.be.false
      })
  })
})

describe('validateArgs()', () => {
  it('should throw a TypeError if the input is not a map or string', () => {
    const testFn = validateArgs.bind(null, false)

    expect(testFn).to.throw(TypeError, 'argument should be a connectionstring or a map of options')
  })

  it('should throw a TypeError if the oplogURL is not specified', () => {
    const testFn = validateArgs.bind(null, {})

    expect(testFn).to.throw(TypeError, 'oplogURL must be specified')
  })

  it('should throw a TypeError if oplogURL is not a string', () => {
    const options = {oplogURL: 12345}
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'oplogURL must be a string')
  })

  it('should set the input string as oplogURL in the output', () => {
    const options = 'test'

    expect(validateArgs(options)).to.have.property('oplogURL').that.deep.equals(options)
  })

  it('should set the input oplogURL in the output', () => {
    const oplogURL = 'test'
    const options = {oplogURL}

    expect(validateArgs(options)).to.have.property('oplogURL').that.deep.equals(oplogURL)
  })

  it('should default getLastTimestamp to getLastTimestamp()', () => {
    const options = {oplogURL: 'test'}

    expect(validateArgs(options)).to.have.property('getLastTimestamp', getLastTimestamp)
  })

  it('should throw a TypeError if getLastTimestamp is not a function', () => {
    const options = {
      oplogURL: 'test',
      getLastTimestamp: 'invalid'
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'getLastTimestamp should be a function that returns a Promise to a Mongo Timestamp')
  })

  it('should default database to .*', () => {
    const options = {oplogURL: 'test'}

    expect(validateArgs(options)).to.have.property('database').that.deep.equals('.*')
  })

  it('should set the input database to the output', () => {
    const database = 'test'
    const options = {
      oplogURL: 'test',
      database
    }

    expect(validateArgs(options)).to.have.property('database').that.deep.equals(database)
  })

  it('should throw a TypeError if database is not a string', () => {
    const options = {
      oplogURL: 'test',
      database: 12345
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'database should be a string')
  })

  it('should default collection to .*', () => {
    const options = {oplogURL: 'test'}

    expect(validateArgs(options)).to.have.property('collection').that.deep.equals('.*')
  })

  it('should set the input collection to the output', () => {
    const collection = 'test'
    const options = {
      oplogURL: 'test',
      collection
    }

    expect(validateArgs(options)).to.have.property('collection').that.deep.equals(collection)
  })

  it('should throw a TypeError if the collection is not a string', () => {
    const options = {
      oplogURL: 'test',
      collection: 12345
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'collection should be a string')
  })

  it('should not default any credentials', () => {
    const options = {oplogURL: 'test'}

    expect(validateArgs(options)).to.not.have.property('credentials')
  })

  it('should set the input credentials to the output', () => {
    const credentials = {
      username: 'username',
      password: 'password'
    }
    const options = {
      oplogURL: 'test',
      credentials
    }

    expect(validateArgs(options)).to.have.property('credentials').that.deep.equals(credentials)
  })

  it('should throw if credentials is not a map', () => {
    const options = {
      oplogURL: 'test',
      credentials: 12345
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'credentials should be provided as an object')
  })

  it('should throw if credentials.username is not provided', () => {
    const credentials = {password: 'password'}
    const options = {
      oplogURL: 'test',
      credentials
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'credentials should have an attribute username that is a string')
  })

  it('should throw if credentials.username is not a string', () => {
    const credentials = {username: 12345}
    const options = {
      oplogURL: 'test',
      credentials
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'credentials should have an attribute username that is a string')
  })

  it('should throw if credentials.password is not provided', () => {
    const credentials = {username: 'username'}
    const options = {
      oplogURL: 'test',
      credentials
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'credentials should have an attribute password that is a string')
  })

  it('should throw if credentials.password is not provided', () => {
    const credentials = {
      username: 'username',
      password: 12345
    }
    const options = {
      oplogURL: 'test',
      credentials
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'credentials should have an attribute password that is a string')
  })

  it('should default retries to 5', () => {
    const options = 'test'

    expect(validateArgs(options)).to.have.property('retries', 5)
  })

  it('should set retries to the output', () => {
    const retries = 10
    const options = {
      oplogURL: 'test',
      retries
    }

    expect(validateArgs(options)).to.have.property('retries', retries)
  })

  it('should throw if retries is not a number', () => {
    const retries = 'retries'
    const options = {
      oplogURL: 'test',
      retries
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'retries should be a number')
  })

  it('should default log to a logging function', () => {
    const options = 'test'

    expect(validateArgs(options)).to.have.property('log').which.is.a.function
  })

  it('should set log to the output', () => {
    const log = console.log
    const options = {
      oplogURL: 'test',
      log
    }

    expect(validateArgs(options)).to.have.property('log', log)
  })

  it('should throw if log is not a function', () => {
    const log = 'log'
    const options = {
      oplogURL: 'test',
      log
    }
    const testFn = validateArgs.bind(null, options)

    expect(testFn).to.throw(TypeError, 'log should be a function that logs strings')
  })
})
