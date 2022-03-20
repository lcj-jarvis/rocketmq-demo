package com.mrlu.rocketmq.delaymsg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 简单de快乐
 * @date 2021-09-13 21:17
 *
 * 发送延迟消息
 */
public class DelayMsgProducer {
    public static final String TOPIC_NAME = "DelayMsgTopic";
    public static final String  MESSAGE_TAG = "delayMsgTag";

    public static void main(String[] args) throws Exception {
        // 创建一个producer，同时设置Producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("DelayMsgProducerGroup");
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
        for (int i = 0; i < 1; i++) {
            byte[] msgBody = ("hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            String msgKeys = "delay-msg-key-" + i;
            /*
               参数一：消息的Topic的名称。
               参数二：消息的标签
               参数三：消息的key
               参数四：消息的内容
             */
            Message message = new Message(TOPIC_NAME, MESSAGE_TAG, msgKeys, msgBody);

            // 设置消息的延时等级，这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(3);

            // 生产者发送消息到Broker
            SendResult sendResult = producer.send(message);
            // 通过SendResult查看消息是否送达
            System.out.println(sendResult);
            System.out.println("消息发送的时间：" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS")));
        }

        // 关闭生产者
        producer.shutdown();
    }
}
