module.exports = {
    ReadConsumerGroup: function(options,topics,callback){
var async = require('async');
var ConsumerGroup = require('kafka-node').ConsumerGroup;
require('events').EventEmitter.prototype._maxListeners = 0;

var buildConsumerOptions = {
    host: options.host,//
    autoCommit: false,
    groupId: options.groupName,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest', // other options 'none', 'earliest'
    commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
    // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
    outOfRangeOffset: 'earliest', // default
    migrateHLC: false,    // for details please see Migration section below
    migrateRolling: true
};
var flag=0;
var count=0;
var counter=0;
var currentTopicName,prvTopicName;
var _messages=[];
var consumerGroup = new ConsumerGroup(Object.assign({id: 'consumer1'}, buildConsumerOptions), topics);
var consumerGroup1 = new ConsumerGroup(Object.assign({id: 'consumer2'}, buildConsumerOptions), topics);
var consumerGroup2 = new ConsumerGroup(Object.assign({id: 'consumer3'}, buildConsumerOptions), topics);
var consumerGroup3 = new ConsumerGroup(Object.assign({id: 'consumer4'}, buildConsumerOptions), topics);
//var consumerGroup4 = new ConsumerGroup(Object.assign({id: 'consumer5'}, buildConsumerOptions), topics);
     try{
            consumerGroup.on('error', onError);
            consumerGroup.on('message', onMessage);
            consumerGroup1.on('error', onError);
            consumerGroup1.on('message', onMessage);
            consumerGroup2.on('error', onError);
            consumerGroup2.on('message', onMessage);
            consumerGroup3.on('error', onError);
            consumerGroup3.on('message', onMessage);
            //consumerGroup4.on('error', onError);
          //  consumerGroup4.on('message', onMessage);
            consumerGroup.on('connect', function(){
              console.log("ConsumerGroup is ready. ");
            });
            consumerGroup1.on('connect', function(){
                console.log("ConsumerGroup1 is ready. ");
            });
            consumerGroup2.on('connect', function(){
                console.log("ConsumerGroup2 is ready. ");
            });
            consumerGroup3.on('connect', function(){
                console.log("ConsumerGroup3 is ready. ");
            });
           // consumerGroup4.on('connect', function(){
           //     console.log("ConsumerGroup4 is ready. ");
          //  });
            function onMessage(message){
                flag=1;
                _messages.push(message);
                async.each([consumerGroup, consumerGroup1,consumerGroup2,consumerGroup3], function (consumer,callback) {
                    consumer.close(true, callback);
                });
            }
            function onError (error) {
                var result={
                    "messages":error,
                    "status":"Some error occured while listening kafka events ",
                    "offsetCount":null
                }   
              callback(result,null)
              async.each([consumerGroup, consumerGroup1,consumerGroup2,consumerGroup3], function (consumer, callback) {
                consumer.close();
            });
            }
            setTimeout(function(){
                if(flag==0){
                    var result={
                        "messages":null,
                        "status":"New message is not available.",
                        "offsetCount":null
                    }   
                    callback(null,result)
                    async.each([consumerGroup, consumerGroup1,consumerGroup2,consumerGroup3], function (consumer, callback) {
                        consumer.close();
                    });}
            else{
                flag=0; 
                var result={
                    "messages":_messages,
                    "status":"Data process",
                    "offsetCount":null
                }       
                callback(null,result);
            }
            },5000)   
            process.once('SIGINT', function(){
                async.each([consumerGroup, consumerGroup1,consumerGroup2,consumerGroup3], function (consumer, callback) {
                    consumer.close(true, callback);
                });
            })
           
        }catch(error){
           console.log("Could not connect to kafka events for build " +error);
        }
    }
};
