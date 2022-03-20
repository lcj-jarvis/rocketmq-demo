package com.mrlu.rocketmq.commonmsg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author 简单de快乐
 * @date 2021-09-12 16:31
 *
 * 单向消息生产者
 */
public class OneWayMsgProducer {
    public static final String TOPIC_NAME = "oneWayMsgTopic";
    public static final String  MESSAGE_TAG = "oneWayMsgTag";
    public static final String PRODUCER_GROUP_NAME = "oneWayMsgProducerGroup";
    public static final String NAME_SERVER_ADDRESS = "192.168.187.129:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);
        producer.setNamesrvAddr(NAME_SERVER_ADDRESS);

        // 设置消息发送超时时间为5s
        producer.setSendMsgTimeout(5000);
        producer.start();

        for (int i = 0; i < 5; i++) {
            byte[] msgBody = ("one way send msg " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            String msgKeys = "one-way-msg-key-" + i;
            Message msg = new Message(TOPIC_NAME, MESSAGE_TAG, msgKeys, msgBody);
            // 单向发送
            producer.sendOneway(msg);
        }

        producer.shutdown();
        System.out.println("Producer shutdown");
    }
}
