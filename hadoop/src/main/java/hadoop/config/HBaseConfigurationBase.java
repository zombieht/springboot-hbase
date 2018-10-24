package hadoop.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

/**
 * @author alan.huang
 */
@org.springframework.context.annotation.Configuration
public class HBaseConfigurationBase {

    private Logger logger= LoggerFactory.getLogger(HBaseConfigurationBase.class);


    @Value("${hbase.zookeeper.quorum}")
    private String zkQuorum;

    @Value("${hbase.master}")
    private String hBaseMaster;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zkPort;

    @Value("${zookeeper.znode.parent}")
    private String znode;



    /**
     * 产生HBaseConfiguration实例化Bean
     * @return
     */
    @Bean
    public Configuration configuration() {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        conf.set("zookeeper.znode.parent", znode);
        conf.set("hbase.master", hBaseMaster);
        logger.info("quorum is :"+zkQuorum);
        return conf;
    }



//    @Bean
//    public HbaseTemplate hbaseTemplate() {
//        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", zkQuorum);
//        configuration.set("hbase.rootdir", this.hbaseProperties.getRootDir());
//        configuration.set("zookeeper.znode.parent", znode);
//        return new HbaseTemplate(configuration);
//    }


    @Bean
    public HbaseTemplate hbaseTemplate() {
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        hbaseTemplate.setConfiguration(configuration());
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }



}
