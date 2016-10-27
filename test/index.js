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
    restore = OplogEmitter.__set__({
      connectToMongo: dummyConnect,
      pollerOptions: {retries: 1}
    })

    function errorCallback (error) {
      expect(error).to.be.an('error')
    }

    let emitter = new OplogEmitter('test')
    emitter.on('error', spy(errorCallback, done))
  })

  it('should emit "insert" when an insert happens', (done) => {
    const doc = createOplogDocument('insert')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('insert', spy(callback, done))
    emitter.on('error', failCallback)
    emitter.on('update', failCallback)
    emitter.on('delete', failCallback)
  })

  it('should emit "update" when an update happens', (done) => {
    const doc = createOplogDocument('update')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('update', spy(callback, done))
    emitter.on('error', failCallback)
    emitter.on('insert', failCallback)
    emitter.on('delete', failCallback)
  })

  it('should emit "delete" when a delete happens', (done) => {
    const doc = createOplogDocument('delete')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
      connectToMongo: connect
    })

    function callback (op) {
      expect(op).to.deep.equal(doc)
    }
    function failCallback (error) {
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('delete', spy(callback, done))
    emitter.on('error', failCallback)
    emitter.on('insert', failCallback)
    emitter.on('update', failCallback)
  })

  it('should emit "op" for an insert event', (done) => {
    const doc = createOplogDocument('insert')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
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
    restore = OplogEmitter.__set__({
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

  it('should emit "op" for a delete event', (done) => {
    const doc = createOplogDocument('delete')
    const cursor = createMongoCursor([doc])
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
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

  it('should emit all events', (done) => {
    const docs = [createOplogDocument('insert'), createOplogDocument('delete'), createOplogDocument('update')]
    const cursor = createMongoCursor(docs)
    const connect = () => {
      return Promise.resolve(cursor)
    }
    restore = OplogEmitter.__set__({
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
      done(error)
    }

    let emitter = new OplogEmitter('test')
    emitter.on('op', callback)
    emitter.on('error', failCallback)
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
    restore = OplogEmitter.__set__({
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
      done(error)
    }

    let emitter = new OplogEmitter({
      oplogURL: 'test',
      database,
      collection
    })
    emitter.on('op', callback)
    emitter.on('error', failCallback)
  })
})

describe('getLastTimestamp()', () => {
  it('should return a Promise', () => {
    expect(getLastTimestamp()).to.be.an.instanceOf(Promise)
  })

  it('should resolve to a Mongodb Timestamp', () => {
    expect(getLastTimestamp()).to.eventually.be.an.instanceOf(Timestamp)
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
})
