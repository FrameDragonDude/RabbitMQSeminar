package com.seminar.rabbitmq.producer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.NotificationMessage;
import com.seminar.rabbitmq.dto.OrderMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final RabbitTemplate rabbitTemplate;

    public OrderProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendOrder(OrderMessage order) {
        rabbitTemplate.convertAndSend(RabbitMQConfig.DIRECT_EXCHANGE, RabbitMQConfig.ORDER_ROUTING_KEY, order, message -> {
            // messageId = orderCode: khoá chống trùng đi kèm message, xem được ở Management UI và ở DLQ
            message.getMessageProperties().setMessageId(order.getOrderCode());
            return message;
        });
    }

    public void broadcast(NotificationMessage notification) {
        // Fanout bỏ qua routing key
        rabbitTemplate.convertAndSend(RabbitMQConfig.FANOUT_EXCHANGE, "", notification, message -> {
            message.getMessageProperties().setMessageId(notification.id());
            return message;
        });
    }
}
