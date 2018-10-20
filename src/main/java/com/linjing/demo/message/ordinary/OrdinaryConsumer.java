package com.linjing.demo.message.ordinary;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author cxc
 * @date 2018/10/18 17:07
 */
public class OrdinaryConsumer {
    public static void main(String[] args) throws MQClientException {
        /**
         * Consumer Group,非常重要的概念，后续会慢慢补充
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_demo");
//指定NameServer地址，多个地址以 ; 隔开
//        consumer.setNamesrvAddr("112.74.43.136:9876;192.168.116.116:9876"); //修改为自己的
        consumer.setNamesrvAddr("112.74.43.136:9876");

/**
 * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
 * 如果非第一次启动，那么按照上次消费的位置继续消费
 */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest", "*");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    for (MessageExt msg : list) {
                        String msgbody = new String(msg.getBody(), "utf-8");
                        System.out.println("  MessageBody: " + msgbody);//输出消息内容
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT; //稍后再试
                }
                return ConsumeOrderlyStatus.SUCCESS; //消费成功
            }
        });


        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
