package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestSpout implements IRichSpout {
	private static Random rand = null;
	protected SpoutOutputCollector collector;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		rand = new Random();
		this.collector = collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		PaymentMessage[] paymentMessages = getMessage();
		for (PaymentMessage paymentMessage : paymentMessages) {
			byte [] body = RaceUtils.writeKryoObject(paymentMessage);
			collector.emit(new Values(body));		
		}
	}
	private PaymentMessage[] getMessage() {
		OrderMessage orderMessage = null;
		PaymentMessage[] paymentMessages = null;
		try {
            int platform = rand.nextInt(2);
            orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
            orderMessage.setCreateTime(System.currentTimeMillis());

            paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
            double amount = 0;
            for (PaymentMessage paymentMessage : paymentMessages) {
            	if (platform == 0) {
            		paymentMessage.setPaySource((short) 4);
            	} else if (platform == 1) {
            		paymentMessage.setPaySource((short) 5);
            	}
                int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                if (retVal < 0) {
                    throw new RuntimeException("price < 0 !!!!!!!!");
                }

                if (retVal > 0) {
                    amount += paymentMessage.getPayAmount();
                }
            }

            if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                throw new RuntimeException("total price is not equal.");
            }


        } catch (Exception e) {
            e.printStackTrace(); 
        }
		return paymentMessages;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("origin"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
