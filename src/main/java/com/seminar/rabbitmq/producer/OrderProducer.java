package com.seminar.rabbitmq.producer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.OrderMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void sendOrder(OrderMessage message) {
        rabbitTemplate.convertAndSend(RabbitMQConfig.DIRECT_EXCHANGE, RabbitMQConfig.ORDER_ROUTING_KEY, message);
    }

    public void sendBroadcast(String message) {
        rabbitTemplate.convertAndSend(RabbitMQConfig.FANOUT_EXCHANGE, "", message);
    }
}
