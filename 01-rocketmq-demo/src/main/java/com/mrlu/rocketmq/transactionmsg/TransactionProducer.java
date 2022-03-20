package com.mrlu.rocketmq.transactionmsg;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.xml.transform.Transformer;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @author 简单de快乐
 * @date 2021-09-14 20:59
 *
 * 使用 TransactionMQProducer类创建生产者，并指定唯一的 ProducerGroup，
 * 就可以设置自定义线程池来处理这些检查请求。执行本地事务后、需要根据执行结果对消息队列进行回复。
 *
 */
public class TransactionProducer {

    public static final String TOPIC_NAME = "TcMsgTopicTest";

    public static void main(String[] args) throws Exception {
        // 事务消息的生产者
        TransactionMQProducer producer = new TransactionMQProducer("TcMsgProducerGroup");
        producer.setNamesrvAddr("192.168.187.129:9876");

        // 创建一个线程池
        ExecutorService executorService = new ThreadPoolExecutor(5,
                Runtime.getRuntime().availableProcessors(), 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        // 为生产者指定线程池
        producer.setExecutorService(executorService);

        //创建事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        // 为生产者添加事务监听器
        producer.setTransactionListener(transactionListener);

        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message msg = new Message(TOPIC_NAME, tags[i % tags.length], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 发送事务消息（相当于开启全局事务的操作）
                // 第二个参数用于指定在执行本地事务时要使用的业务场景
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.println("TransactionId: " + msg.getTransactionId());
                // System.out.printf("%s%n", sendResult);
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        producer.shutdown();
    }

}
