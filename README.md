# Event-Bus
Event-Bus is a Node.js client with Zookeeper integration for Apache Kafka 0.8.1 and later.
Using this we can send and receive messages.

Features:
1)Send message to kafka server and save that message into given topic.
2)If topic is not present then it create topic with 1 partition.
3)Get messages which saved by producer using consumer.
4)Get messages using consumer group also.

API:
1)Import this package into your application and create its object with two parameters like hostName and GroupName.
 const wrapper=require('pegasuseventbus');
 var config={
  connectiobString:'127.0.0.1:2181',
  groupName:'some_test_topic',
  kafkaHost:'127.0.0.1:9092'
}
 const obj=new wrapper(config);

2) Using above object we can send and receive messages from topics.
   send messages to topic:
   obj.send({_topicName:"DemoTest",_message:"Hi Friends cccc"},function(err,data){
       console.log(data);
   })
   _topicName is name of topic in which we stores message.
   This function returns boject with topicName,partition and offset.
   
 3)Read messages from topics.
   obj.receive("Test",function(err,data){
    console.log(data.messages);
    console.log(data.status);
})
 First parameter of this function is topic name.
 4)Read messages using consumerGroup
     var topics = ['Test','DemoTest'];
  obj.bulkReceive(topics,function(err,data){
     console.log(data.messages)
})
topics is a array of topics.
This function reads message from both the topics at a same time.
