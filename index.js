var Promise = require('bluebird')
var mongodb = require('mongodb')
var seconds = require('juration').parse
var MongoClient = mongodb.MongoClient;
var Db = mongodb.Db;
var Collection = mongodb.Collection;

Promise.promisifyAll(Collection.prototype);
Promise.promisifyAll(Db.prototype);
Promise.promisifyAll(MongoClient);

var DEFAULT_MONGO_URL = 'mongodb://127.0.0.1:27017/kev'
var DEFAULT_COLLECTION = 'kev'

var ID_KEY = "key"
var DATA_FIELD_KEY = "value"

var connections = {}
var dbs = []
var clients = []

var KevMongo = module.exports = function KevMongo(options) {
  if (!(this instanceof KevMongo)) return new KevMongo(options)
  options = options || {}
  var collection = this.collection = options.collection || DEFAULT_COLLECTION
  if (options.db) {
    var db = options.db
    this.db = Promise.resolve()
      .then(function () { return db })
      .then(function (db) {
        if (!db.createCollectionAsync) return Promise.promisifyAll(db)
        else return db
      })
  } else {
    var url = options.url || DEFAULT_MONGO_URL
    var collection = options.collection || DEFAULT_COLLECTION

    if (!connections[url]) {
      connections[url] = MongoClient.connectAsync(url, options.options || {})
    }

    this.db = connections[url]
    this.url = url
  }
  var self = this
  this.storage = this.db.then(function (db) {
    self.db = db
    return db.createCollectionAsync(collection).catch(function (err) {
      if (~err.message.indexOf('collection already exists')) return
      else throw err
    }).then(function (collection) {
      if (!~dbs.indexOf(db)) {
        clients[dbs.length] = []
        dbs.push(db)
      }
      clients[dbs.indexOf(db)].push(collection)
      return collection
    })
  }).then(function (collection) {
    var index = {}
    index[ID_KEY] = 1
    if (!collection.createIndexAsync) collection = Promise.promisifyAll(collection)
    collection.createIndex(index, { background: true })
    collection.createIndex({ expiresAt: 1 }, { background: true, expireAfterSeconds: 0 })
    return collection
  })
  if (options.ttl) this.ttl = seconds(String(options.ttl))
}

KevMongo.prototype.put = function put (key, value, done) {
  var query = {}
  query[ID_KEY] = key

  var update = {}
  update[ID_KEY] = key
  update[DATA_FIELD_KEY] = value
  if (this.ttl !== undefined) {
    update.expiresAt = new Date(Date.now() + this.ttl * 1000)
  }

  this.storage.then(function (collection) {
    return collection.findAndModifyAsync(query, [], update, { upsert: true })
  }).then(function(result) {
    if (done) done(null, result.value ? result.value[DATA_FIELD_KEY] : null)
  }).catch(function (err) {
    if (done) done(err)
  })
}

KevMongo.prototype.get = function get (key, done) {
  var ttl = this.ttl
  this.storage.then(function (collection) {
    if (Array.isArray(key)) {
      var query = {}
      query[ID_KEY] = { $in: key }
      return collection.findAsync(query).then(function (cursor) {
        var out = {}
        var values = cursor.toArray(function (err, values) {
          if (err && done) return done(err)
          if (err) throw err
          values.forEach(function (v) {
            if (v.expiresAt && v.expiresAt < new Date(Date.now())) return
            out[v[ID_KEY]] = v[DATA_FIELD_KEY]
          })
          done && done(null, out)
        })
      }).catch(function (err) { done && done(err) })
    } else {
      var query = {}
      query[ID_KEY] = key
      return collection.findOneAsync(query).then(function(doc) {
        done && !doc && done()
        done && doc.expiresAt && doc.expiresAt < new Date(Date.now()) && done()
        done && done(null, doc[DATA_FIELD_KEY])
      }).catch(function (err) { done && done(err) })
    }
  })
}

KevMongo.prototype.del = function del (key, done) {
  var query = {}
  query[ID_KEY] = key
  this.storage.then(function (collection) {
    return collection.findAndModifyAsync(query, [], {}, { remove: true }).then(function (result) {
      var value = result.value ? result.value[DATA_FIELD_KEY] : null
      if (done) done(null, value)
    }).catch(function (err) { done && done(err) })
  })
}

KevMongo.prototype.close = function (done) {
  var self = this
  this.storage.then(function (collection) {
    var db = self.db
    var db_clients = clients[dbs.indexOf(db)]
    db_clients.splice(db_clients.indexOf(collection), 1)
    if (db_clients.length === 0) {
      var index = dbs.indexOf(db)
      dbs.splice(index, 1)
      clients.splice(index, 1)
      self.url && delete connections[self.url]
      db.close(done)
    } else {
      done && done()
    }
  })
}
