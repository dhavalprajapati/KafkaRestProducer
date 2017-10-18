package org.icc.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private final int mySessionTimeOut = 15 * 1000;
    private final int myConnectionTimeOut = 10 * 1000;
    private final int myPartitions = 2;


    private final String myTopic;
    private final String myZookeeperHosts;
    private final String myBrokerList;
    private final int myReplicationFactor;
    private Writer writer;

    public Producer (String topic, String zookeeperHosts, String brokerList){
        this.myTopic = topic;
        this.myZookeeperHosts = zookeeperHosts;
        this.myBrokerList = brokerList;
        myReplicationFactor = myBrokerList.split(",").length;
        createTopicIfExist();
    }

    private void createTopicIfExist() {
        ZkClient zkClient = new ZkClient(myZookeeperHosts, mySessionTimeOut, myConnectionTimeOut, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(myZookeeperHosts), false);
        if (!AdminUtils.topicExists(zkUtils, myTopic)) {
            AdminUtils.createTopic(zkUtils, myTopic, myPartitions, 1, new Properties(), new RackAwareMode.Disabled$());
            zkClient.close();
        } else {
            LOGGER.info("{} is available hence no changes are done");
        }
    }

    public Writer getWriter() {
        if(writer == null) {
            writer = new Writer(myTopic, myBrokerList);
        }
        return writer;
    }

}
