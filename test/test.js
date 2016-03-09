var Promise = require('bluebird')
var Kev = require('kev')
var KevMongo = require('../index.js')
var assert = require('assert')
var mongoose = require('mongoose')

function run_test(kev) {
  return kev.putAsync('key1', 'value1').then(function() {
    return kev.getAsync('key1')
  }).then(function(value) {
    assert.equal(value, 'value1')
  }).then(function () {
    return kev.delAsync('key1')
  }).then(function(old) {
    assert.equal(old, 'value1')
  }).then(function () {
    return kev.getAsync('key1')
  }).then(function (value) {
    assert.equal(value, null)
  }).then(function () {
    var max = 2000
    var puts = []
    for (var i = 1; i <= max; i++) {
      puts.push(kev.putAsync(String(i), i))
    }
    return Promise.all(puts)
  }).then(function () {
    console.log('put all')
    var max = 2000
    var keys = []
    for (var i = 1; i <= max; i++) {
      keys.push(String(i))
    }
    return kev.getAsync(keys)
  }).then(function (values) {
    console.log('got all')
    assert.equal(values['100'], 100)
    assert.equal(values['2000'], 2000)
  }).then(function (values) {
    return kev.putAsync('key2', 'to-expire')
  }).then(function () {
    return kev.getAsync('key2')
  }).then(function (value) {
    assert.equal(value, 'to-expire')
    return Promise.delay(6000)
  }).then(function () {
    return kev.getAsync('key2')
  }).then(function(value) {
    assert.equal(value, null)
    kev.close()
    console.log('PASSED')
  })
}

var MongoClient = Promise.promisifyAll(require('mongodb').MongoClient)
MongoClient
  .connectAsync(process.env.MONGO_URL + '/kev-test2')
  .then(function (db) {
    // Verify it accepts an existing mongo connection
    return Promise.promisifyAll(Kev({ store: KevMongo({ db: db, ttl: '5 sec' }) }))
  })
  .then(function (kev) {
    return run_test(kev)
  })
  .then(function () {
    // Verify it can handle more than one open mongo connection
    var mongo = KevMongo({
      ttl: 5,
      db: mongoose
        .connect(process.env.MONGO_URL)
        .then(() => mongoose.connection.db)
    })
    return Promise.promisifyAll(Kev({ store: mongo }))
  })
  .then(function (kev) {
    return run_test(kev)
  })
  .then(function () {
    var mongo = KevMongo( { url: process.env.MONGO_URL + '/kev-test', ttl: 5 } )
    var kev = Promise.promisifyAll(Kev({ store: mongo }))
    // Verify it accepts urls
    run_test(kev)
  })
  .then(function () {
    // Verify url pooling
    var mongo1 = KevMongo( { url: process.env.MONGO_URL + '/kev-test', ttl: 5 } )
    var mongo2 = KevMongo( { url: process.env.MONGO_URL + '/kev-test', ttl: 5 } )
    assert.equal(mongo1.db, mongo2.db)
    mongo1.close(() => mongo2.close())
  })
