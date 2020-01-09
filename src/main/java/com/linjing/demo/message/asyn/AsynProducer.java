package com.linjing.demo.message.asyn;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;
import java.util.Date;
public class AsynProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_demo-1");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 0; i <= 100; i++) {
            Message msg = new Message("TopicTest-1", "TagA", "keys", ("测试RocketMQ" + i).getBytes("UTF-8")
            );
            try {
                // 异步发送消息, 发送结果通过 callback 返回给客户端。
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        // 消费发送成功
                        System.out.println("SUCCESS信息:" + sendResult.toString());
                        System.out.println("send message success. topic=" + sendResult.getRegionId() + ", msgId=" + sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
                        System.out.println("FAIL信息:" + throwable.getMessage());
                    }
                });
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
