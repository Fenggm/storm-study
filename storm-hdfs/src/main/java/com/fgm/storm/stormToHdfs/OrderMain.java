package com.fgm.storm.stormToHdfs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @Auther: fgm
 */
public class OrderMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        //source:  http://storm.apache.org/releases/1.1.2/storm-hdfs.html

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        //指定我们的文件上传到hdfs的哪个路径,注意这个路径不存在需要自己创建,并且要改变这个路径的权限
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/stormhdfs/");

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://node01:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomOrderSpout", new RandomOrderSpout());
        builder.setBolt("countMoneyBolt", new CountMoneyBolt()).localOrShuffleGrouping("randomOrderSpout");
        builder.setBolt("hdfsBolt", bolt).localOrShuffleGrouping("countMoneyBolt");

        Config config=new Config();
        //调整我们异或算法的线程个数,
        config.setNumAckers(5);

        /**
         * 如果线程池当中的消息太多都消不掉,说明代码可能存在问题
         * 设置我们内存池当中最多有5000个消息没有消掉,就不要再发送数据了,继续发送,会导致oom的异常
         */
        config.setMaxSpoutPending(5000);


        if (null!=args && args.length>0){
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else{
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("stormHdfs",config,builder.createTopology());
        }

    }
}
