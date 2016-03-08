var Promise = require('bluebird')
var Kev = require('kev')
var KevMongo = require('../index.js')
var assert = require('assert')
var mongoose = require('mongoose')

var kev = Promise.promisifyAll(Kev({ store: KevMongo( { url: process.env.MONGO_URL + '/kev-test' } ) }))

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
    console.log('PASSED')
    kev.close()
  })
}

var MongoClient = Promise.promisifyAll(require('mongodb').MongoClient)
MongoClient
  .connectAsync(process.env.MONGO_URL + '/kev-test2')
  .then(function (db) {
    return Promise.promisifyAll(Kev({ store: KevMongo({ db: db }) }))
  })
  .then(function (kev) {
    return run_test(kev)
  })
  .then(function () {
    return Promise.promisifyAll(Kev({ store: KevMongo({ db: mongoose.connect(process.env.MONGO_URL).then(function () { return mongoose.connection.db }) }) }))
  })
  .then(function (kev) {
    return run_test(kev)
  })
  .then(function () {
    run_test(kev)
  })
