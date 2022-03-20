package com.mrlu.rocketmq.ordermsg;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author 简单de快乐
 * @date 2021-09-12 21:19
 */
public class UseMsgKeyAsSelectKeyConsumer {

    public static final String TOPIC_NAME = "orderMsgTopic02";
    public static final String NAME_SERVER_ADDRESS = "192.168.187.129:9876";


    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer= new DefaultMQPushConsumer("order_msg_producer_group");
        consumer.setNamesrvAddr(NAME_SERVER_ADDRESS);

        consumer.subscribe(TOPIC_NAME, "TagA || TagC || TagD");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        final String orderType = "COMPUTER";
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                Random random = new Random();
                msgs.forEach(msg -> {
                    String keys = msg.getKeys();

                    if (keys.contains(orderType)) {
                        System.out.println("consumeThread=" + Thread.currentThread().getName() + ", queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));

                    }
                    try {
                        //模拟业务逻辑处理中...
                        TimeUnit.SECONDS.sleep(random.nextInt(10));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer Started.");
    }
}
