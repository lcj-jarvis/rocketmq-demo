package com.mrlu.rocketmq.commonmsg;

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
 * @date 2021-09-12 16:27
 *
 * 异步消息消费者
 */
public class AsyncMsgConsumer {

    public static final String TOPIC_NAME = "asyncMsgTopic";
    public static final String  MESSAGE_TAG = "asyncMsgTag";
    public static final String CONSUMER_GROUP_NAME = "AsyncMsgConsumerGroup";
    public static final String NAME_SERVER_ADDRESS = "192.168.187.129:9876";

    public static void main(String[] args) throws MQClientException {

        // 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP_NAME);

        // 指定NameServer
        consumer.setNamesrvAddr(NAME_SERVER_ADDRESS);

        // 从最后的consumerOffset次开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

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
                    System.out.println(msg);
                    System.out.println(msg.getKeys() + " 消息体：" + new String(msg.getBody()));
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
