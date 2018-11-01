var chai=require('chai')
var expect=chai.expect;
var should=chai.should();
const wrapper=require('./Wrapper');
const app=new wrapper('127.0.0.1:2181','some_test_topic');
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
      app.receive('Test',function(err,data){
        expect(data.status).to.be.a('string');
        expect(data.messages).to.be.a('array');
        expect(data.status).to.equal('Data process');
        return done();
    })
  })
})
describe('Consumer Group',function(){
  it('should read message from topic using consumer group',function(done){
    app.ConsumerGroup(['build_status_events'],function(err,data){
      expect(data.status).to.be.a('string');
      expect(data.messages).to.be.a('array');
      expect(data.status).to.equal('Data process');
        return done();
    })
  })
})
