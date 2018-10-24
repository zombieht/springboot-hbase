package hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author alan.huang
 */
@Component
public class HconnectionFactory implements InitializingBean {
    @Override
    public void afterPropertiesSet() throws Exception {

    }

//    protected final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());
//
//    @Value("${hbase.zookeeper.quorum}")
//    private String zkQuorum;
//
//    @Value("${hbase.master}")
//    private String hBaseMaster;
//
//    @Value("${hbase.zookeeper.property.clientPort}")
//    private String zkPort;
//
//    @Value("${zookeeper.znode.parent}")
//    private String znode;
//
//    private static Configuration conf = HBaseConfiguration.create();
//
//    public static Connection connection;
//
//    @Override
//    public void afterPropertiesSet(){
//        conf.set("hbase.zookeeper.quorum", zkQuorum);
//        conf.set("hbase.zookeeper.property.clientPort", zkPort);
//        conf.set("zookeeper.znode.parent", znode);
//        conf.set("hbase.master", hBaseMaster);
//        conf.setAllowNullValueProperties(true);
//        try {
//            connection = ConnectionFactory.createConnection(conf);
//            logger.info("获取connectiont连接成功！");
//        } catch (IOException e) {
//            e.printStackTrace ();
//            logger.error("获取connectiont连接失败！");
//        }
//    }
//

//    @Bean
//    public HbaseTemplate hbaseTemplate() {
//        HbaseTemplate hbaseTemplate = new HbaseTemplate();
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", zkQuorum);
//        conf.set("hbase.zookeeper.property.clientPort", zkPort);
//        conf.set("zookeeper.znode.parent", znode);
//        conf.set("hbase.master", hBaseMaster);
//        hbaseTemplate.setConfiguration(conf);
//        hbaseTemplate.setAutoFlush(true);
//
//        // hbaseTemplate.get("test1", "0001", null);
//
//        logger.info("获取connectiont连接成功！------------------------");
//        return hbaseTemplate;
//    }


}