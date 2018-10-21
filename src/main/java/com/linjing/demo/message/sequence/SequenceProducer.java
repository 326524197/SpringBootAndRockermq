package com.linjing.demo.message.sequence;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

/**
 * @author cxc
 * @date 2018/10/18 17:07
 * rockerMq发送顺序消息
 */
public class SequenceProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_demo");
        producer.setNamesrvAddr("112.74.43.136:9876");
        producer.start();

        for (int i = 0; i <= 100; i++) {
            int orderId = i % 10;
            Message msg = new Message("TopicTest", "TagA", "keys", ("测试RocketMQ" + i).getBytes("UTF-8")
            );
            try {
                //延迟发送等级
                msg.setDelayTimeLevel(2);
                // 顺序发送消息。
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                            @Override
                            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                                // arg的值其实就是orderId
                                Integer id = (Integer) o;
                                // mqs是队列集合，也就是topic所对应的所有队列
                                int index = id % list.size();
                                // 这里根据前面的id对队列集合大小求余来返回所对应的队列
                                return list.get(index);
                            }
                        }
                        , orderId);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
                System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}
