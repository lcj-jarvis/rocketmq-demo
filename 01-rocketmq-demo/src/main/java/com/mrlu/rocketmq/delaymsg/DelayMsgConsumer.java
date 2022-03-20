package com.mrlu.rocketmq.delaymsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author 简单de快乐
 * @date 2021-09-13 21:19
 */
public class DelayMsgConsumer {
    public static final String TOPIC_NAME = "DelayMsgTopic";
    public static final String  MESSAGE_TAG = "delayMsgTag";


    public static void main(String[] args) throws MQClientException {
        // 定义一个pull消费者
        // DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("SyncMsgConsumerGroup");

        // 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("DelayMsgConsumerGroup");

        // 指定NameServer
        consumer.setNamesrvAddr("192.168.187.129:9876");

        // 指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 线程池数量动态调整阈值.即消费者实例达到多少后开始扩容线程池
        // consumer.setAdjustThreadPoolNumsThreshold(200000);

        // 指定消费的Topic与Tag
        consumer.subscribe(TOPIC_NAME, MESSAGE_TAG);
        // 采用“广播模式”进行消费，默认为“集群模式”
        // consumer.setMessageModel(MessageModel.BROADCASTING);

        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 逐条消费
                for (MessageExt msg : msgs) {
                    System.out.println("消息消费的时间：" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS")));
                    System.out.println(msg);
                }
                // 返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer started");
    }
}
