package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class RaceRatioBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RaceRatioBolt.class);
    private OutputCollector collector;
    private HashMap<Long, Double> pcResult = null;
    private HashMap<Long, Double> mobileResult = null;     
	private List<String> confServers = null;
	private DefaultTairManager tairManager = null;   
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pcResult = new HashMap<Long, Double>();
        this.mobileResult = new HashMap<Long, Double>();
        this.confServers = new ArrayList<String>();
        this.tairManager = new DefaultTairManager();
    	this.confServers.add(RaceConfig.TairConfigServer);
    	this.tairManager.setConfigServerList(confServers);
    	this.tairManager.setGroupName(RaceConfig.TairGroup);
    	this.tairManager.init();
    }
    @Override
    public void execute(Tuple tuple) {
    	PaymentMessage payMessage = (PaymentMessage) tuple.getValue(0);
    	Long time = payMessage.getCreateTime();
    	Double money = payMessage.getPayAmount();
    	short platformPaySource = payMessage.getPaySource();   	
    	  	
    	if (platformPaySource == 0) {
	    	if (pcResult.containsKey(time)) {
	    		Long curIterTime = time;
	    		pcResult.put(curIterTime, pcResult.get(curIterTime) + money);
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(time), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (pcResult.containsKey(curIterTime)) {
	    			pcResult.put(curIterTime, pcResult.get(curIterTime) + money);
		    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(time), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}else {
	    		pcResult.put(time, money); 
	    		if (mobileResult.containsKey(time))
	    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(time), mobileResult.get(time) / pcResult.get(time));	
	    	}  		
    	} else if (platformPaySource == 1) {
	    	if (mobileResult.containsKey(time)) {
	    		Long curIterTime = time;
	    		mobileResult.put(curIterTime, mobileResult.get(curIterTime) + money);
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(time), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (mobileResult.containsKey(curIterTime)) {
	    			mobileResult.put(curIterTime, mobileResult.get(curIterTime) + money);
		    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(time), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}else {
	    		mobileResult.put(time, money); 	
	    		if (pcResult.containsKey(time))
	    			tairManager.put(RaceConfig.TairNamespace, "ratio_" + Long.toString(time), mobileResult.get(time) / pcResult.get(time));
	    	}  		  		
    	}
    	this.collector.ack(tuple);
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
    
}
