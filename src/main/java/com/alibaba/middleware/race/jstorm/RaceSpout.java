package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
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

public class RaceSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
		MessageListenerConcurrently {
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = Logger.getLogger(RaceSpout.class);
	private HashMap<Long, String> platformID = null;
	private int i = 0;
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
		this.platformID = new HashMap<Long, String>();
		
	

		try {
			consumer = new DefaultMQPushConsumer();
			consumer.setConsumerGroup(RaceConfig.MetaConsumerGroup);
			//consumer.setInstanceName(RaceConfig.MetaConsumerGroup + "@" + i++);
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
	        //consumer.setNamesrvAddr(RaceConfig.RocketMQAddress);
	        consumer.subscribe(RaceConfig.MqPayTopic, "*");
	        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
	        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
	        consumer.registerMessageListener(this);
	        consumer.start();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create Consumer :" + i++, e);
		}

		if (consumer == null) {
			LOG.warn(id
					+ " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
						LOG.info(sb.toString());
					}
				}
			}).start();
		}
		
		LOG.info("Successfully init " + id);
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
			LOG.info("take from payQueue fail");
		}
		List<Object> tuples = new ArrayList<Object>();
		if (payTuple == null) {
			return;
		}else {
			if (platformID.get(payTuple.getOrderId()) == "taobao") {
				payTuple.setPaySource((short) 4);
				tuples.add(payTuple);
				collector.emit(tuples,UUID.randomUUID());
				tuples.clear();				
			}else if (platformID.get(payTuple.getOrderId()) == "tmall") {
				payTuple.setPaySource((short) 5);
				tuples.add(payTuple);
				collector.emit(tuples,UUID.randomUUID());
				tuples.clear();		
			}else {
				payQueue.offer(payTuple);
			}
			LOG.info("emit success");
		}
		
	}

	@Deprecated
	public void ack(Object msgId) {
		LOG.warn("Shouldn't go this ack function");
	}

	@Deprecated
	public void fail(Object msgId) {
		LOG.warn("Shouldn't go this fail function");
	}
	
	@Override
	public void ack(Object msgId, List<Object> values) {
		
	}
	
	@Override
	public void fail(Object msgId, List<Object> values) {
		payQueue.offer((PaymentMessage)(values.get(0))); 
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
                    //Info: 生产者停止生成数据, 并不意味着马上结束
                	LOG.info("Got the end signal of RocketMQ");
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
                else
                {
                	OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                	platformID.put(tmallMessage.getOrderId(), "tmall");
                }
            	LOG.info("Got the " + msg.getTopic() + " message from RocketMQ");               
            }
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		} catch (Exception e) {
			LOG.error("Failed to got the information from MQ " + id, e);
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

}
