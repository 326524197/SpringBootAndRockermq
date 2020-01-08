package com.linjing.demo.message.oneway;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * @author cxc
 * @date 2018/10/18 17:07
 * rockerMq发送单向消息
 */
public class OneWayProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_demo");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 0; i <= 10; i++) {

            Message msg = new Message("TopicTest", "TagA", "keys", ("测试RocketMQ" + i).getBytes("UTF-8")
            );
            try {
                // 由于在 oneway 方式发送消息时没有请求应答处理，一旦出现消息发送失败，则会因为没有重试而导致数据丢失。若数据不可丢，建议选用可靠同步或可靠异步发送方式。
                producer.sendOneway(msg);
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
