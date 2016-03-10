var Promise = require('bluebird')
var mongodb = require('mongodb')
var seconds = require('juration').parse
var resurrect = require('./resurrect')()
var zlib = require('zlib')
var globber = require('glob-to-regexp')
var MongoClient = mongodb.MongoClient
Promise.promisifyAll(mongodb.Collection.prototype)
Promise.promisifyAll(mongodb.Db.prototype)
Promise.promisifyAll(mongodb.Cursor.prototype)
Promise.promisifyAll(MongoClient)

var DEFAULT_MONGO_URL = 'mongodb://127.0.0.1:27017/kev'
var DEFAULT_COLLECTION = 'kev'

var ID_KEY = "key"
var DATA_FIELD_KEY = "value"

var connections = {}
var dbs = []
var clients = []
var expired = (r) => r && r.expiresAt && r.expiresAt < new Date(Date.now())

var KevMongo = module.exports = function KevMongo (options) {
  if (!(this instanceof KevMongo)) return new KevMongo(options)
  options = options || {}
  this.collection = options.collection || DEFAULT_COLLECTION
  if (options.ttl) options.ttl = seconds(String(options.ttl))

  if (options.db) {
    this.db = Promise.resolve()
      .then(() => options.db)
      .then((db) => {
        if (!db.createCollectionAsync) return Promise.promisifyAll(db)
        else return db
      })
  } else {
    var url = options.url || DEFAULT_MONGO_URL
    if (!connections[url]) {
      connections[url] = MongoClient.connectAsync(url, options.options || {})
    }
    this.db = connections[url]
    this.url = url
  }

  this.options = options
  this.storage = this.db.then((db) => {
    this.db = db
    return db.createCollectionAsync(this.collection)
    .catch((err) => {
      if (~err.message.indexOf('collection already exists')) return
      else throw err
    })
    .then((collection) => {
      if (!~dbs.indexOf(db)) {
        clients[dbs.length] = []
        dbs.push(db)
      }
      clients[dbs.indexOf(db)].push(collection)
      return collection
    })
  }).then((collection) => {
    if (!collection.createIndexAsync) collection = Promise.promisifyAll(collection)
    var index = {}
    index[ID_KEY] = 1
    collection.createIndex(index, { background: true })
    collection.createIndex({ expiresAt: 1 }, { background: true, expireAfterSeconds: 0 })
    return collection
  })
}

KevMongo.prototype.get = function get (keys, done) {
  var ttl = this.ttl
  var query = {}
  query[ID_KEY] = { $in: keys }
  this.storage.then((db) => db.findAsync(query))
    .then((r) => Promise.fromCallback(r.toArray.bind(r)))
    .filter((r) => r && !expired(r))
    .reduce((out, v) => { out[v[ID_KEY]] = unpack(this.options.compress)(v[DATA_FIELD_KEY]); return out }, {})
    .props()
    .then((out) => done && done(null, out))
    .catch((err) => done && done(err))
}

KevMongo.prototype.put = function put (keys, options, done) {
  this.storage.then((db) => {
    var ttl = options.ttl || this.options.ttl
    for (key in keys) {
      var query = { [ID_KEY]: key }
      var update = { [ID_KEY]: key }
      if (ttl) update.expiresAt = new Date(Date.now() + ttl * 1000)
      keys[key] = pack(this.options.compress)(keys[key])
        .then((v) => update[DATA_FIELD_KEY] = v)
        .then(() => db.findOneAndReplaceAsync(query, update, { upsert: true }))
        .then((r) => (r && r.value && !expired(r.value)) ? r.value[DATA_FIELD_KEY] : null)
        .then(unpack(this.options.compress))
    }
    return Promise.props(keys)
      .then((v) => done && done(null, v))
      .catch((e) => done && done(e))
  })
}

KevMongo.prototype.del = function del (keys, done) {
  this.storage.then((db) => {
    return Promise.resolve(keys)
      .reduce((out, key) => {
        out[key] = db.findOneAndDeleteAsync({ [ID_KEY]: key })
          .then((r) => (r && r.value) ? r.value[DATA_FIELD_KEY] : null)
          .then(unpack(this.options.compress))
        return out
      }, {})
      .props()
      .then((v) => done && done(null, v))
      .catch((e) => done && done(e))
  })
}

KevMongo.prototype.drop = function drop (pattern, done) {
  var re = globber(pattern)
  this.storage
    .then((db) => db.deleteManyAsync({ [ID_KEY]: { $regex: re } }))
    .then((r) => done && done(null, r.deletedCount))
    .catch((e) => done && done(e))
}

KevMongo.prototype.close = function (done) {
  this.storage.then((collection) => {
    var db = this.db
    var db_clients = clients[dbs.indexOf(db)]
    db_clients.splice(db_clients.indexOf(collection), 1)
    if (db_clients.length === 0) {
      var index = dbs.indexOf(db)
      dbs.splice(index, 1)
      clients.splice(index, 1)
      this.url && delete connections[this.url]
      db.close(done)
    } else {
      done && done()
    }
  })
}

function pack (compress) {
  return Promise.promisify((value, done) => {
    if (!value || !compress) return setImmediate(() => done(null, value))
    zlib.deflate(resurrect.stringify(value), compress, (err, buf) => {
      if (err) done(err)
      else done(null, buf.toString('base64'))
    })
  })
}

function unpack (compress) {
  return Promise.promisify((value, done) => {
    if (!value || !compress) return setImmediate(() => done(null, value))
    zlib.inflate(new Buffer(value, 'base64'), compress, (err, val) => {
      if (err) done(err)
      else done(null, resurrect.resurrect(val.toString()))
    })
  })
}
