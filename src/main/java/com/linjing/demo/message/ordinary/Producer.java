package com.linjing.demo.message.ordinary;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * @author cxc
 * @date 2018/10/18 17:07
 * rockerMq发送普通消息
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_demo");
        //指定NameServer地址
        //修改为自己的
        //多个可以用";"隔开
        //producer.setNamesrvAddr("192.168.116.115:9876;192.168.116.116:9876");
        producer.setNamesrvAddr("112.74.43.136:9876");

        /*
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();

        for (int i = 0; i <= 100; i++) {

                /*
                构建消息
                参数  topic:   Message 所属的 Topic
                      tags:   可理解为对消息进行再归类，方便 Consumer 指定过滤条件在 MQ 服务器过滤
                      keys:   设置代表消息的业务关键属性，请尽可能全局唯一,
                              以方便您在无法正常收到消息情况下，可通过阿里云服务器管理控制台查询消息并补发
                              注意：不设置也不会影响消息正常收发
                      body:    Body 可以是任何二进制形式的数据， MQ 不做任何干预，
                               需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
                 */
            Message msg = new Message("TopicTest", "TagA", "keys", ("测试RocketMQ" + i).getBytes("UTF-8")
            );
            try {
                //发送同步消息
                SendResult sendResult = producer.send(msg);
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
