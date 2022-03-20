package com.mrlu.rocketmq.commonmsg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * @author 简单de快乐
 * @date 2021-09-12 16:12
 *
 * 异步消息生产者
 */
public class AsyncMsgProducer {

    public static final String TOPIC_NAME = "asyncMsgTopic";
    public static final String  MESSAGE_TAG = "asyncMsgTag";
    public static final String PRODUCER_GROUP_NAME = "AsyncMsgProducerGroup";
    public static final String NAME_SERVER_ADDRESS = "192.168.187.129:9876";

    public static void main(String[] args) throws Exception {
        // 创建生产者，并设置生产者组的名字
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);
        // 设置nameServer的地址
        producer.setNamesrvAddr(NAME_SERVER_ADDRESS);

        //指定异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(2);
        // 指定新创建的Topic的Queue的数量为2，默认是为4
        producer.setDefaultTopicQueueNums(2);

        // 开启生产者
        producer.start();

        int count = 5;
        for (int i = 0; i < count; i++) {
            byte[] msgBody = ("send asyncMsg " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            String msgKeys = "async-msg-key-" + i;
            Message message = new Message(TOPIC_NAME, MESSAGE_TAG, msgKeys, msgBody);
            // 异步发送指定回调
            producer.send(message, new SendCallback() {
                // 当producer接收到MQ发送过来的ACK后就会触发该回调方法的执行
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                // 发生异常是才会触发该回调方法执行
                @Override
                public void onException(Throwable e) {
                    System.out.println("send error");
                    e.printStackTrace();
                }
            });
        }

        // sleep一会
        // 由于采用的是异步发送，所以若在这里不sleep，则消息还未发送就会将producer给关闭，报错
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
        System.out.println("Producer shutdown");
    }
}
