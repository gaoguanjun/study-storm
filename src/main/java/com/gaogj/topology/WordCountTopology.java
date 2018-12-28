/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gaogj.topology;

import com.gaogj.spout.WordCountSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {

  /**
   * 分词bolt
   */
  public static class SplitSentence  extends BaseBasicBolt {

    String sentenceStr ;
    String sentenceArray [] ;
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try{
            sentenceStr = tuple.getStringByField("word");
            if(null!=sentenceStr){
              sentenceArray = sentenceStr.split(" ");
              for(int i = 0;i<sentenceArray.length;i++){
                basicOutputCollector.emit(new Values(sentenceArray[i]));
              }
            }
        }catch (Exception e){
            throw new FailedException("ack failed !");
        }

    }
  }

  /**
   * 单词计数bolt
   */
  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      System.err.println(Thread.currentThread().getName()+"\t单词："+ word + "\t：\t"+ count);
      collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }


  /**
   * 单词总数
   */
  public static class WordSum extends BaseBasicBolt {
    Map<String, Integer> countSum = new HashMap<String, Integer>();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      try{
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        long sum = 0;
        countSum.put(word,count);
        for(String key:countSum.keySet()){
          sum += countSum.get(key);
        }
        System.err.println("-----------\t"+sum+"\t-------");
      }catch (Exception e){
        throw new FailedException("");
      }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new WordCountSpout(), 1);
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
    builder.setBolt("sum", new WordSum(), 1).shuffleGrouping("count");
    Config conf = new Config();
   // conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      //conf.setMaxTaskParallelism(3);
      conf.setNumWorkers(1);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      //Thread.sleep(10000);

      //cluster.shutdown();
    }
  }
}
