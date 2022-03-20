package com.mrlu.rocketmq.ordermsg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author 简单de快乐
 * @date 2021-09-12 20:59
 *
 * 解决hash冲突
 */
public class UseMsgKeyAsSelectKeyProducer {

    public static final String TOPIC_NAME = "orderMsgTopic02";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_msg_producer_group");

        producer.setNamesrvAddr("192.168.187.129:9876");

        producer.setSendMsgTimeout(5000);

        producer.start();

        //设置消息的标签
        String[] tags = new String[]{"TagA", "TagC", "TagD"};

        // 订单列表
        List<OrderStep> orderList = new UseMsgKeyAsSelectKeyProducer().buildOrders();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);
        for (int i = 0; i < 10; i++) {
            // 加个时间前缀
            String body = dateStr + " Hello RocketMQ " + orderList.get(i);
            // 消息的key
            String msgKey = orderList.get(i).getOrderId() + "-" + orderList.get(i).getOrderType();
            Message msg = new Message(TOPIC_NAME, tags[i % tags.length], msgKey, body.getBytes());

            // send()方法的第三个参数会传递给select()方法的第三个参数
            // 指定消息选择器.具体的选择算法在这里写.
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    // 使用消息的key作为选择key的选择算法
                    String msgKeys = msg.getKeys();
                    // 根据hash值进行取模,选择queue进行投递
                    int index = getMsgKeysHashCode(msgKeys) % mqs.size();
                    System.out.println(index);
                    return mqs.get(index);
                }
            }, orderList.get(i).getOrderId());//订单id

            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body));
        }

        producer.shutdown();
    }

    public static int getMsgKeysHashCode(String msgKeys) {
        // 避免数组下标越界
        int hashCode = msgKeys.hashCode();
        if (hashCode < 0) {
            return -hashCode;
        }
        return  hashCode;
    }

    /**
     * 订单的步骤
     */
    private static class OrderStep {
        /**
         * 订单号
         */
        private long orderId;

        /**
         * 订单顺序
         */
        private String desc;

        /**
         * 订单类型
         */
        private OrderType orderType;

        public long getOrderId() {
            return orderId;
        }

        public void setOrderId(long orderId) {
            this.orderId = orderId;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        public OrderType getOrderType() {
            return orderType;
        }

        public void setOrderType(OrderType orderType) {
            this.orderType = orderType;
        }

        @Override
        public String toString() {
            return "OrderStep{" +
                    "orderId=" + orderId +
                    ", desc='" + desc + '\'' +
                    ", orderType=" + orderType +
                    '}';
        }
    }

    /**
     * 生成模拟订单数据
     */
    private List<OrderStep> buildOrders() {
        List<OrderStep> orderList = new ArrayList<>();

        OrderStep orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("创建");
        orderDemo.setOrderType(OrderType.COMPUTER);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("创建");
        orderDemo.setOrderType(OrderType.PHONE);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("付款");
        orderDemo.setOrderType(OrderType.COMPUTER);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("创建");
        orderDemo.setOrderType(OrderType.WATCH);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("付款");
        orderDemo.setOrderType(OrderType.PHONE);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("付款");
        orderDemo.setOrderType(OrderType.WATCH);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("完成");
        orderDemo.setOrderType(OrderType.PHONE);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("推送");
        orderDemo.setOrderType(OrderType.COMPUTER);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("完成");
        orderDemo.setOrderType(OrderType.WATCH);
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("完成");
        orderDemo.setOrderType(OrderType.COMPUTER);
        orderList.add(orderDemo);

        return orderList;
    }

    /**
     * 订单类型，作为消息的key
     */
    enum OrderType {
        COMPUTER,PHONE,WATCH
    }
}
