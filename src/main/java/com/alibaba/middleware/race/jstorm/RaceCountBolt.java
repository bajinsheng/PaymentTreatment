package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.taobao.tair.Result;
import com.taobao.tair.impl.DefaultTairManager;

public class RaceCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private HashMap<Long, Double> taobaoResult = null;
    private HashMap<Long, Double> tmallResult = null;    
	private List<String> confServers = null;
	private DefaultTairManager tairManager = null;   
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taobaoResult = new HashMap<Long, Double>();
        this.tmallResult = new HashMap<Long, Double>();
        try {
	        this.confServers = new ArrayList<String>();
	        this.tairManager = new DefaultTairManager();
	    	this.confServers.add(RaceConfig.TairConfigServer);
	    	this.tairManager.setConfigServerList(confServers);
	    	this.tairManager.setGroupName(RaceConfig.TairGroup);
	    	this.tairManager.init();     	
        } catch (Exception e) {
        	throw new RuntimeException("Failed to initial Tair in Count Bolt", e);
        }
    }
    @Override
    public void execute(Tuple tuple) {
    	PaymentMessage payMessage = (PaymentMessage) tuple.getValue(0);
    	Long time = payMessage.getCreateTime();
    	Double money = payMessage.getPayAmount();
    	short platformID = payMessage.getPayPlatform();
    	if (platformID == 4) {   		
	    	if (taobaoResult.containsKey(time)) {
	    		taobaoResult.put(time, taobaoResult.get(time) + money);
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_taobao + Long.toString(time), taobaoResult.get(time));
	    	}else {
	    		taobaoResult.put(time, money); 	
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_taobao + Long.toString(time), money);	
	    	}  		
    	} else if (platformID == 5) {
	    	if (tmallResult.containsKey(time)) {
	    		tmallResult.put(time, tmallResult.get(time) + money);
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_tmall + Long.toString(time), tmallResult.get(time));
	    	}else {
	    		tmallResult.put(time, money); 	
	    		tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_tmall + Long.toString(time), money);	
	    	}  		  		
    	}
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
    

}
