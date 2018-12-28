package com.gaogj.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Administrator on 2018/12/22.
 */
public class MySpout implements IRichSpout{

    FileInputStream fileInputStream;
    InputStreamReader inputStreamReader;
    BufferedReader bufferedReader;
    SpoutOutputCollector spoutOutputCollector;
    String str = null;
    String messageId = null;
    private HashMap<String, String> waitAck = new HashMap<String, String>();

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mySpoutFileStr"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            this.fileInputStream = new FileInputStream("mySpoutFile");
            this.inputStreamReader = new InputStreamReader(fileInputStream,"Utf-8");
            this.bufferedReader = new BufferedReader(inputStreamReader);
            this.spoutOutputCollector = spoutOutputCollector;
            }catch (Exception e){
                e.printStackTrace();
        }


    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {

        try {
            while((str=this.bufferedReader.readLine())!=null){
                messageId = UUID.randomUUID().toString().replace("-", "");
                waitAck.put(messageId,str);  //放到缓存中
                spoutOutputCollector.emit(new Values(str),messageId);
                //Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ack(Object o) {
        waitAck.remove(o.toString());
        System.err.println("spout  成功 "+ o.toString() +"  waitAck容量："+ waitAck.size());
    }

    public void fail(Object o) {
        System.err.println("spout  失败 "+ o.toString() );
        spoutOutputCollector.emit(new Values(waitAck.get(o.toString())),messageId +"  waitAck容量："+ waitAck.size());
    }
}
