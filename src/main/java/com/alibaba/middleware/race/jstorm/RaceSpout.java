package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

public class RaceSpout implements IRichSpout, MessageListenerConcurrently {
	private Hashtable<Long, String> platformID = null;
	protected SpoutOutputCollector collector;
	protected String id;
	protected transient DefaultMQPushConsumer consumer;
	protected transient LinkedBlockingDeque<PaymentMessage> payQueue;

	public RaceSpout() {

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.payQueue = new LinkedBlockingDeque<PaymentMessage>();
		this.platformID = new Hashtable<Long, String>();
		try {
			consumer = new DefaultMQPushConsumer();
			consumer.setConsumerGroup(RaceConfig.MetaConsumerGroup);
			consumer.setInstanceName(RaceConfig.MetaConsumerGroup + "@" + UUID.randomUUID());
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.setConsumeMessageBatchMaxSize(RaceConfig.ConsumeMessageBatchMaxSize);
			consumer.setConsumeThreadMin(RaceConfig.ConsumeThreadMin);
			consumer.setConsumeThreadMax(RaceConfig.ConsumeThreadMax);
			consumer.setPullBatchSize(RaceConfig.ConsumePullBatchSize);
	        //consumer.setNamesrvAddr(RaceConfig.RocketMQAddress);
	        consumer.subscribe(RaceConfig.MqPayTopic, "*");
	        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
	        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
	        consumer.registerMessageListener(this);
	        consumer.start();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create Consumer", e);
		}
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}

	}

	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}

	@Override
	public void nextTuple() {
		PaymentMessage payTuple = null;
		try {
			payTuple = payQueue.take();
		} catch (InterruptedException e) {
			return;
		}
		if (platformID.get(payTuple.getOrderId()) == "taobao") {
			payTuple.setPaySource((short) 4);
			collector.emit(new Values(RaceUtils.writeKryoObject(payTuple)));
			
		}else if (platformID.get(payTuple.getOrderId()) == "tmall") {
			payTuple.setPaySource((short) 5);
			collector.emit(new Values(RaceUtils.writeKryoObject(payTuple)));
		}else {
			payQueue.offer(payTuple);
		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin"));
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		try {	
			for (MessageExt msg : msgs) {
                byte [] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    continue;
                }
                if (msg.getTopic() == RaceConfig.MqPayTopic)
                {
                	payQueue.offer(RaceUtils.readKryoObject(PaymentMessage.class, body));
                }
                else if (msg.getTopic() == RaceConfig.MqTaobaoTradeTopic)
                {
                	OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                	platformID.put(taobaoMessage.getOrderId(), "taobao");
                }
                else if (msg.getTopic() == RaceConfig.MqTmallTradeTopic)
                {
                	OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                	platformID.put(tmallMessage.getOrderId(), "tmall");
                }             
            }
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		} catch (Exception e) {
			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}
	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

}
