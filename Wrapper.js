module.exports =class Message{
    constructor(config) {
        this.connectiobString = config.connectiobString;//Zookeeper connection string, default localhost:2181/
        this.kafkaHost = config.kafkaHost;//A string of kafka broker/host combination delimited by comma
        this.groupName = config.groupName;//consumer group id
      }
      createTopic(topics,callback){ //This function create topic with given configurations.
          var options={
            kafkaHost:this.kafkaHost
          }
        var kafka = require('kafka-node'),
         client = new kafka.KafkaClient(options);
        client.createTopics(topics,function(err,res){
            if(err){
                var result={
                    data:err,
                    status:"Topic creation is in error state."
                }
                callback(result,null);
            }else{
                var result={
                    data:res,
                    status:"Topic created successfully."
                }
                callback(null,result);
            }
        })
   }
   send(_data,callback){ //This function save message to given topic.
       var _config={host:this.connectiobString,groupName:this.groupName};
    Message.prototype.sendMessage(_config,_data,function(err,data){
       callback(err,data);
    })
  }
   receive(_topicName,callback){//This function read message from given topic
    var _config={host:this.connectiobString,groupName:this.groupName}
    Message.prototype.getMessage(_config,_topicName,function(err,data){
        callback(err,data);
     })
  }
   commitOffset(_config,_topic,callback){
    var kafka = require('kafka-node'),
    client = new kafka.Client(_config.host),
    offset = new kafka.Offset(client);
    if(_topic.partition ==undefined && _topic.partition==null)
    _topic.partition=0;
    offset.commit(_config.groupName, [
        { topic: _topic.topicName, partition: _topic.partition, offset: _topic.offset }
    ], function (err, data) {
          
    });
  }
   bulkReceive(topics,callback){//This function reads message from multiple topics.
    const ConsumerGroup=require('./ConsumerGroup');
    ConsumerGroup.ReadConsumerGroup(this,topics,function(err,data){
       callback(err,data);
    })
  }
   sendMessage(_config,_data,callback){
    var kafka=require('kafka-node');
    var Producer = kafka.Producer,
    client = new kafka.Client(_config.host), 
    producer = new Producer(client);
      producer.on('ready', function () {
       var payloads = [
            { topic: _data._topicName, messages:_data._message ,timestamp: Date.timestamp }
        ];    
        producer.send(payloads, function (err, data) { 
            var result={
                data:data,
                status:"Message send successfully."
            }        
         callback(null,result)
         producer.close();
      }); 
      });       
      producer.on('error', function (err) {
          console.log('Producer is in error state: '+err);       
          var result={
            data:err,
            status:"Producer is in error state."
        } 
          callback(result,null);
          producer.close();
      })     

  }

   getMessage(_config,_topicName,callback){
    var counter=0; 
    var _messages=[];
    var _partition=0;
    var _offset;
    var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client(_config.host),
    offset = new kafka.Offset(client);
    offset.fetchCommits(_config.groupName, [
        { topic: _topicName, partition: _partition }
    ], function (err, data) {
        _offset=data[_topicName][_partition];
       if(_offset==-1 || _offset==0){
            _offset=0;
       }else{
        _offset=_offset+1;
       }
       console.log(_offset)
    });
    setTimeout(function(){
    var consumer = new Consumer(
        client,
        [
            { topic: _topicName,offset:_offset,partition:_partition}
        ], 
        {
            autoCommit: false,
            fetchMaxWaitMs: false,
            fetchMinBytes: 1,
            fetchMaxBytes: 1024 * 1024,
            fromOffset: true,
            groupId: _config.groupName,
        }
    );  
    var flag=0; 
    consumer.on('message', function (message) {  
        flag=1; 
        if(counter==0){
            counter=message.offset+1;
        } else{counter++;}
        _messages.push(message);
        if(counter==message.highWaterOffset){
            console.log(counter);
            Message.prototype.commitOffset(_config,{topicName:_topicName,offset:message.highWaterOffset-1,partition:_partition},function(err,data){
                 console.log(data)
                })
            var result={
                "messages":_messages,
                "status":"Data process",
                "offsetCount":message.highWaterOffset-1
            }          
            callback(null,result)
            counter=0;
            consumer.close();
        }          
    });   
    consumer.on('error', function (err) {
        var result={
            "messages":err,
            "status":"Consumer is in error state",
            "offsetCount":0
        }
        callback(result,null)
        consumer.close();
    });
    setTimeout(function(){
        if(flag==0){
            var result={
                "messages":null,
                "status":"New message is not available.",
                "offsetCount":0
            }
            callback(null,result)
        consumer.close();}
    else{
        flag=0; 
    }
    },500)   
    
},1000)
  }
}
