package com.fgm.storm.cron;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;

/**
 *
 * @Auther: fgm
 */
public class CronMySqlMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/log_monitor");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "user";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

       /**JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName("user")
                .withQueryTimeoutSecs(30);
        Or*/
        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into user values (?,?,?)")
                .withQueryTimeoutSecs(30);




        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomLineSpout",new RandomCronSpout());
        builder.setBolt("cronBolt",new CronBolt()).localOrShuffleGrouping("randomLineSpout");
        builder.setBolt("jdbcBolt",userPersistanceBolt).localOrShuffleGrouping("cronBolt");



        Config config = new Config();
        if(null != args && args.length > 0){
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("cronTopo",config,builder.createTopology());


        }

    }
}
