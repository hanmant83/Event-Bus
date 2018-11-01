var chai=require('chai')
var expect=chai.expect;
var should=chai.should();
const wrapper=require('./Wrapper');
var config={
  connectiobString:'127.0.0.1:2181',
  groupName:'some_test_topic',
  kafkaHost:'127.0.0.1:9092'
}
const app=new wrapper(config);
describe('Create topic',function(){
  it('should create topic',function(done){
    var topics=[{
      topic: 'topic1',
      partitions: 2,
      replicationFactor: 2
    }]
    app.createTopic(topics,function(err,data){
      expect(data.status).to.be.a('string');
      expect(data.status).to.equal('Topic created successfully.');
        return done();
    })
  })
})
describe('Producer',function(){
  it('should send message to topic successfully',function(done){
    var _data={_topicName:"build_status_events",_message:"Hi Friends cccc"}
      app.send(_data,function(err,data){
        console.log(data.status)
        expect(data.status).to.be.a('string');
        expect(data.data).to.be.a('object');
        expect(data.status).to.equal('Message send successfully.');
        return done();
    })
  })
})
describe('Consumer',function(){
  it('should read message from topic',function(done){
      app.receive('build_status_events',function(err,data){
        expect(data.status).to.be.a('string');
        expect(data.messages).to.be.a('array');
        expect(data.status).to.equal('Data process');
        return done();
    })
  })
})
describe('Consumer Group',function(){
  it('should read message from topic using consumer group',function(done){
    app.bulkReceive(['build_status_events','Test'],function(err,data){
      expect(data.status).to.be.a('string');
      expect(data.messages).to.be.a('array');
      expect(data.status).to.equal('Data process');
        return done();
    })
  })
})
