package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RaceBufferBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
    	PaymentMessage payMessage = RaceUtils.readKryoObject(PaymentMessage.class, tuple.getBinaryByField("timestamp"));
    	byte [] body = RaceUtils.writeKryoObject(payMessage);
    	collector.emit(new Values(body,payMessage.getCreateTime()));
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("buffer", "time"));
    }
}
