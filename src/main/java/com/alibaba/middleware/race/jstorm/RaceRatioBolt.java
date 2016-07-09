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
    private OutputCollector collector;
    private HashMap<Long, Double> pcResult = null;
    private HashMap<Long, Double> mobileResult = null;     
	private List<String> confServers = null;
	private DefaultTairManager tairManager = null;   
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pcResult = new HashMap<Long, Double>();
        this.mobileResult = new HashMap<Long, Double>();
        try {
	        this.confServers = new ArrayList<String>();
	        this.tairManager = new DefaultTairManager();
	    	this.confServers.add(RaceConfig.TairConfigServer);
	    	this.tairManager.setConfigServerList(confServers);
	    	this.tairManager.setGroupName(RaceConfig.TairGroup);
	    	this.tairManager.init();     	
        } catch (Exception e) {
        	throw new RuntimeException("Failed to initial Tair in Ratio Bolt", e);
        }
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
    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (pcResult.containsKey(curIterTime)) {
	    			pcResult.put(curIterTime, pcResult.get(curIterTime) + money);
	    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}else {
	    		Long curIterTime = time;
	    		pcResult.put(curIterTime, money);
    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (pcResult.containsKey(curIterTime)) {
	    			pcResult.put(curIterTime, pcResult.get(curIterTime) + money);
	    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}  		
    	} else if (platformPaySource == 1) {
	    	if (mobileResult.containsKey(time)) {
	    		Long curIterTime = time;
	    		mobileResult.put(curIterTime, mobileResult.get(curIterTime) + money);
	    		if (pcResult.containsKey(time))
	    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (mobileResult.containsKey(curIterTime)) {
	    			mobileResult.put(curIterTime, mobileResult.get(curIterTime) + money);
		    		if (pcResult.containsKey(time))
		    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}else {
	    		Long curIterTime = time;
	    		mobileResult.put(curIterTime, money);
	    		if (pcResult.containsKey(time))
	    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		curIterTime++;
	    		while (mobileResult.containsKey(curIterTime)) {
	    			mobileResult.put(curIterTime, mobileResult.get(curIterTime) + money);
		    		if (pcResult.containsKey(time))
		    			tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + Long.toString(curIterTime), mobileResult.get(curIterTime) / pcResult.get(curIterTime));
	    		}
	    	}  		  		
    	}
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
    
}
