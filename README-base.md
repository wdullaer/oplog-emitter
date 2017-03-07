# oplog-emitter [![NPM version][npm-image]][npm-url] [![Build Status][travis-image]][travis-url] [![Dependency Status][daviddm-image]][daviddm-url] [![Greenkeeper badge][greenkeeper-image]](https://greenkeeper.io/)

> Process your mongodb oplog as events

The module connects to a mongodb oplog and emits all of the transactions as a nodejs event emitter. This provides a much nicer high level API to work with.
All events are emitted as an `op` event. Insert, update and delete events are available as `insert`, `update` and `delete` respectively.

The module can authenticate in the admin database using username/password and allows you to pass in a custom last processed timestamp. If this is not provided, it will only emit events that occurred after the module connected to mongodb.

## Installation

```sh
$ npm install oplog-emitter
```

## Usage

```js
let OplogEmitter = require('oplog-emitter');

let emitter = new OplogEmitter('mongodb://myuser:password@localhost:27000/local?authSource=admin')
emitter.on('insert', (op) => console.log(`${op} was inserted`))
```

## TODO
* Allow more mongodb authentication mechanisms

## License

Apache-2.0 © [Wouter Dullaert](https://wdullaer.com)


[npm-image]: https://badge.fury.io/js/oplog-emitter.svg
[npm-url]: https://npmjs.org/package/oplog-emitter
[travis-image]: https://travis-ci.org/wdullaer/oplog-emitter.svg?branch=master
[travis-url]: https://travis-ci.org/wdullaer/oplog-emitter
[daviddm-image]: https://david-dm.org/wdullaer/oplog-emitter.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/wdullaer/oplog-emitter
[greenkeeper-image]: https://badges.greenkeeper.io/wdullaer/oplog-emitter.svg
[greenkeeper-url]: https://greenkeeper.io/

## API
