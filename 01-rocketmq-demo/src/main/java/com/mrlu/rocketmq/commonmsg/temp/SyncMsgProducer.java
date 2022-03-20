package com.mrlu.rocketmq.commonmsg.temp;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author 简单de快乐
 * @date 2021-09-11 22:09
 *
 * 前置工作：
 * 1、centos-7.8下启动nameServer和Broker
 * 2、运行E:\入门微服务\RocketMQ\自己做的笔记\单Broker控制台jar包(centos-7.8下的rocketmq客户端)\rocketmq-console-ng-1.0.0.jar
 * 3、通过localhost:7200访问监控界面
 *
 * 同步发送消息的生产者
 */
public class SyncMsgProducer {

    public static final String TOPIC_NAME = "syncMsgTopic";
    public static final String  MESSAGE_TAG = "syncMsgTag";

    public static void main(String[] args) throws Exception {
        // 创建一个producer，同时设置Producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("SyncMsgProducerGroup");
        // 指定NameServer的地址。rocketmq安装在centos-7.8下，记得关闭防火墙
        producer.setNamesrvAddr("192.168.187.129:9876");

        // 设置当前发送失败是重试发送的次数，默认为两次
        producer.setRetryTimesWhenSendFailed(3);

        // 设置发送超时时间5s，默认为3s
        producer.setSendMsgTimeout(5000);

        // 设置Queue的数量，如果不设置的话，默认是4个
        // producer.setDefaultTopicQueueNums(2);

        // 开启生产者
        producer.start();

        // 生产并发送5条消息
        for (int i = 0; i < 100; i++) {
            byte[] msgBody = ("hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            String msgKeys = "sync-msg-key-" + i;
            /*
               参数一：消息的Topic的名称。
               参数二：消息的标签
               参数三：消息的key
               参数四：消息的内容
             */
            Message message = new Message(TOPIC_NAME, MESSAGE_TAG, msgKeys, msgBody);
            // 生产者发送消息到Broker
            SendResult sendResult = producer.send(message);
            // 通过SendResult查看消息是否送达
            System.out.println(sendResult);
        }

        // 关闭生产者
        producer.shutdown();
    }
}
