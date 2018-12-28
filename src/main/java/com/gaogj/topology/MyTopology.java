package com.gaogj.topology;

import com.gaogj.bolt.MyBolt;
import com.gaogj.spout.MySpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/12/22.
 */
public class MyTopology {
    public static  void main(String[]args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("MySpout",new MySpout(),1);
        //builder.setBolt("MyBolt",new MyBolt(),2).shuffleGrouping("MySpout"); //1、shuffleGrouping 轮训分组模式
        //builder.setBolt("MyBolt",new MyBolt(),2).noneGrouping("MySpout");   // 2、（随机分派）
        //builder.setBolt("MyBolt",new MyBolt(),2).allGrouping("MySpout");     // 3、（广播发送，即每一个Tuple，每一个Bolt都会收到）
        builder.setBolt("MyBolt",new MyBolt(),2).globalGrouping("MySpout");     // 4、全局分组，将Tuple分配到task id值最低的task里面）
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 1);
       // conf.put(Config.TOPOLOGY_DEBUG, true);
       // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS,1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, builder.createTopology());
        Utils.sleep(60*1000);
        cluster.shutdown();

    }
}
