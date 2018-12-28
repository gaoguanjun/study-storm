package com.gaogj.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Administrator on 2018/12/22.
 */
public class MyBolt implements IRichBolt{

    String receiveStr = null;
    long i = 0;
    OutputCollector outputCollector = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        try {
            receiveStr = tuple.getStringByField("mySpoutFileStr");
            System.err.println(++i+  "行" + "\t"+Thread.currentThread().getName() + receiveStr);
            this.outputCollector.ack(tuple);
        }catch ( Exception e){
            this.outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
