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
package com.gaogj.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Random;

public class WordCountSpout extends BaseRichSpout {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountSpout.class);

  SpoutOutputCollector _collector;
  Random _rand;
  String sentence;
  String[] sentences;
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    sentences  = new String[]{"a b c d", "b c d e",
            "c d e f"};
  }

  public void nextTuple() {

    for(int i = 0;i< sentences.length;i++){
      sentence  = sentences[i];
     System.err.println("Emitting tuple: "+ sentence + "\t" + sentences.length);
      _collector.emit(new Values(sentence));
    }
      Utils.sleep(60*60*1000);
  }



  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}
