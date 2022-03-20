package com.mrlu.rocketmq.transactionmsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author 简单de快乐
 * @date 2021-09-14 21:35
 */
public class TransactionMsgConsumer {

    public static final String TOPIC_NAME = "TcMsgTopicTest";
    public static final String  MESSAGE_TAG = "*";

    public static void main(String[] args) throws MQClientException {
        // 定义一个pull消费者
        // DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("SyncMsgConsumerGroup");

        // 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TcMsgConsumerGroup");

        // 指定NameServer
        consumer.setNamesrvAddr("192.168.187.129:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // 指定消费的Topic与Tag
        consumer.subscribe(TOPIC_NAME, MESSAGE_TAG);


        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 逐条消费
                for (MessageExt msg : msgs) {
                    System.out.println("TransactionId=" + msg.getTransactionId() + ", queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                    // System.out.println(msg);
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
