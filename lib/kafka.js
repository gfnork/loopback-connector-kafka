'use strict';

var kafka = require('kafka-node');
var Client = kafka.Client;
var Producer = kafka.HighLevelProducer;

exports.initialize = function (dataSource, cb) {
  var settings = dataSource.settings;
  var client = new Client(settings.connectionString, settings.clientId, settings.zkOptions);
  var producer = new Producer(client);
  producer.on('ready', function () {
    console.log('Producer ready');
  });

  producer.on('error', function (err) {
    console.log(err);
  });
  dataSource.connector = new Kafka(producer, settings);
  cb && cb();
};

/**
 *  @constructor
 *  Constructor for KAFKA connector
 *  @param {Object} The kafka-node producer
 */
function Kafka(producer, settings) {
  this.name = 'kafka';
  this._models = {};
  this.producer = producer;
  this.settings = settings;
  this.topic = settings.topic;
}

Kafka.prototype.create = function (model, data, options, cb) {
  var topic = this.topic;
  var producer = this.producer;
  var messages = data;
  var stringify = function (json) {
    return typeof json === 'string' ? json : JSON.stringify(json);
  };
  messages = Array.isArray(messages)
    ? messages.map(function (item) {
      return stringify(item);
    })
    : stringify(messages);
  producer.send([{
    topic: topic,
    messages: messages
  }], cb);
};