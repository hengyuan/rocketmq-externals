package com.sprucetec.data.spark;


import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Rocketmq3Producer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("streamtest");
        producer.setNamesrvAddr("192.168.2.210:9876;192.168.2.211:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            try {
                {
                    Message msg = new Message("testtopic",
                            "TagA",
                            "order",
                            "{\"body\":\"aaa\"}".getBytes("utf-8"));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }

}
