package com.mrlu.rocketmq.filtermsg;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author 简单de快乐
 * @date 2021-09-15 23:32
 *
 * 使用sql过滤
 */
public class SqlMsgProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("SqlMsgProducerGroup");
        producer.setNamesrvAddr("192.168.187.129:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("SqlMsgTopic", "myTagA", body);
            msg.putUserProperty("age", String.valueOf(i));
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        }

    }
}
