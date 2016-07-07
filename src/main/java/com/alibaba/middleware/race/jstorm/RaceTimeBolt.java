package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.model.PaymentMessage;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RaceTimeBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RaceTimeBolt.class);
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
    	PaymentMessage payMessage = (PaymentMessage) tuple.getValue(0);
    	long timestamp = payMessage.getCreateTime() / 1000 / 60;
    	payMessage.setCreateTime(timestamp);
        List<Object> values = new ArrayList<Object>();
        values.add(payMessage);
        values.add(timestamp);
    	collector.emit(tuple, values);
    	LOG.info("Time assign success");
    	this.collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("timestamp", "time"));
    }
}

