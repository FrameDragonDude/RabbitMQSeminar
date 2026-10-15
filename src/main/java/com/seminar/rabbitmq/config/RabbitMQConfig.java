package com.seminar.rabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String DIRECT_EXCHANGE = "order.direct.exchange";
    public static final String FANOUT_EXCHANGE = "system.fanout.exchange";
    public static final String DLX_EXCHANGE = "order.dlx.exchange";

    public static final String ORDER_QUEUE = "order.queue";
    public static final String NOTIFICATION_QUEUE = "notification.queue";
    public static final String DLQ_QUEUE = "order.dlq";

    public static final String ORDER_ROUTING_KEY = "order.created";
    public static final String DLQ_ROUTING_KEY = "order.dead";

    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(DIRECT_EXCHANGE);
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DLX_EXCHANGE);
    }

    // Queue chính có tích hợp cấu hình Dead Letter Exchange khi lỗi
    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(ORDER_QUEUE)
                .withArgument("x-dead-letter-exchange", DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Queue notificationQueue() {
        return new Queue(NOTIFICATION_QUEUE, true);
    }

    @Bean
    public Queue deadLetterQueue() {
        return new Queue(DLQ_QUEUE, true);
    }

    @Bean
    public Binding bindingOrder(Queue orderQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(orderQueue).to(directExchange).with(ORDER_ROUTING_KEY);
    }

    @Bean
    public Binding bindingNotification(Queue notificationQueue, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(notificationQueue).to(fanoutExchange);
    }

    @Bean
    public Binding bindingDLQ(Queue deadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(DLQ_ROUTING_KEY);
    }
}
