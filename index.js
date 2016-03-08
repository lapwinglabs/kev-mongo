var Promise = require('bluebird')
var mongodb = require('mongodb')
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

var KevMongo = module.exports = function KevMongo(options) {
  if (!(this instanceof KevMongo)) return new KevMongo(options)
  options = options || {}

  if (options.db) {
    var db = options.db
    var collection = options.collection || DEFAULT_COLLECTION
    this.storage = Promise.resolve().then(function () { return db })
      .then(function (db) {
        if (!db.createCollectionAsync) return Promise.promisifyAll(db)
        else return db
      })
      .then(function (db) {
        return db.createCollectionAsync(collection)
      })
      .then(function (col) {
        var index = {}
        index[ID_KEY] = 1
        if (!col.createIndexAsync) col = Promise.promisifyAll(col)
        return col.createIndexAsync(index, { background: true }).then(function () { return col })
      })
  } else if (options.url) {
    var url = options.url || DEFAULT_MONGO_URL
    var collection = options.collection || DEFAULT_COLLECTION

    if (!connections[url]) {
      connections[url] = { db: MongoClient.connectAsync(url, {}), collections: {}, clients: [] }
    }

    if (!(connections[url].collections[collection])) {
      connections[url].collections[collection] =
        connections[url].db.then(function (db) {
          return db.createCollectionAsync(collection)
        }).then(function (col) {
          var index = {}
          index[ID_KEY] = 1
          return col.createIndexAsync(index, { background: true }).then(function () { return col })
        })
    }
    this.storage = connections[url].collections[collection]
    this.url = url
    this.collection = collection
    connections[url].clients.push(this)
  }
}

KevMongo.prototype.put = function put (key, value, done) {
  var query = {}
  query[ID_KEY] = key

  var update = {}
  update[ID_KEY] = key
  update[DATA_FIELD_KEY] = value

  this.storage.then(function (collection) {
    return collection.findAndModifyAsync(query, [], update, { upsert: true })
  }).then(function(result) {
    if (done) done(null, result.value ? result.value[DATA_FIELD_KEY] : null)
  }).catch(function (err) {
    if (done) done(err)
  })
}

KevMongo.prototype.get = function get (key, done) {
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
            out[v[ID_KEY]] = v[DATA_FIELD_KEY]
          })
          done(null, out)
        })
      }).catch(function (err) { done && done(err) })
    } else {
      var query = {}
      query[ID_KEY] = key
      return collection.findOneAsync(query).then(function(doc) {
        done && done(null, doc ? doc[DATA_FIELD_KEY] : null)
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
  if (!this.url) return

  var index = connections[this.url].clients.indexOf(this)
  connections[this.url].clients.splice(index, 1)

  if (connections[this.url].clients.length == 0) {
    connections[this.url].db.then(function (db) { db.close() })
    delete connections[this.url]
  }

  if (done) done()
}
