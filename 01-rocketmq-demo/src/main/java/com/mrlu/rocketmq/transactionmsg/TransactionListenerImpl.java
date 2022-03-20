package com.mrlu.rocketmq.transactionmsg;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 简单de快乐
 * @date 2021-09-14 21:10
 *
 * 当发送半消息成功时，我们使用 executeLocalTransaction 方法来执行本地事务。
 * checkLocalTransaction 方法用于检查本地事务状态，并回应消息队列的检查请求。
 */
public class TransactionListenerImpl implements TransactionListener {

    // 实际是将本地事务状态放到本地消息表中，回查状态就去查这个表

    private AtomicInteger transactionIndex = new AtomicInteger(0);
    private ConcurrentHashMap<String, Integer> localTransaction = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     *  这里的executeLocalTransaction(Message msg, Object arg)的参数有producer的以下方法传过来的
     *  producer.sendMessageInTransaction(msg, arg);
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("预先提交消息成功：" + msg);
        // 返回上一次的值，再加一
        int value = transactionIndex.getAndIncrement();
        int status = value % 3;

        // System.out.println(msg.getTransactionId() + "==>" +new String(msg.getBody()) + "==>" + status);

        localTransaction.put(msg.getTransactionId(), status);
        /*if ((value + 1) == 10) {
            System.out.println("================localTransaction===============");
            localTransaction.forEach((k,v) -> System.out.println(k + " : " + v));
            System.out.println("================localTransaction===============");

        }*/
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 回查本地事务的状态
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Integer status = localTransaction.get(msg.getTransactionId());
        if (status == null) {
            System.out.println();
            System.out.println("status == null " + msg.getTransactionId() + "==>" + localTransaction);
        }
        System.out.println(msg.getTransactionId() + "==>" +new String(msg.getBody()) + "==>" + status);
        if (null != status) {
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
