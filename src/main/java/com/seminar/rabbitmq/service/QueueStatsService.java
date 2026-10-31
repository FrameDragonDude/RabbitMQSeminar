package com.seminar.rabbitmq.service;

import com.seminar.rabbitmq.dto.QueueStat;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.seminar.rabbitmq.config.RabbitMQConfig.*;

/**
 * Đọc số tin Ready và số consumer của từng queue trực tiếp từ broker.
 */
@Service
public class QueueStatsService {

    private record Route(String queue, String exchange, String exchangeType, String routingKey) {
    }

    private static final List<Route> ROUTES = List.of(
            new Route(ORDER_QUEUE, DIRECT_EXCHANGE, "direct", ORDER_ROUTING_KEY),
            new Route(DLQ_QUEUE, DLX_EXCHANGE, "direct", DLQ_ROUTING_KEY),
            new Route(EMAIL_QUEUE, FANOUT_EXCHANGE, "fanout", ""),
            new Route(SMS_QUEUE, FANOUT_EXCHANGE, "fanout", ""),
            new Route(AUDIT_QUEUE, FANOUT_EXCHANGE, "fanout", ""));

    private final AmqpAdmin amqpAdmin;

    public QueueStatsService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    public List<QueueStat> queueStats() {
        return ROUTES.stream().map(this::toStat).toList();
    }

    /** Số tin trong queue cũ, {@code null} nếu queue đó không còn tồn tại. */
    public Integer legacyQueueReady() {
        QueueInformation info = amqpAdmin.getQueueInfo(LEGACY_NOTIFICATION_QUEUE);
        return info == null ? null : info.getMessageCount();
    }

    private QueueStat toStat(Route route) {
        QueueInformation info = amqpAdmin.getQueueInfo(route.queue());
        int ready = info == null ? 0 : info.getMessageCount();
        int consumers = info == null ? 0 : info.getConsumerCount();
        return new QueueStat(route.queue(), route.exchange(), route.exchangeType(), route.routingKey(), ready, consumers);
    }
}
