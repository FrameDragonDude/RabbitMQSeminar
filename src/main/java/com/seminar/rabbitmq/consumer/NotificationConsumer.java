package com.seminar.rabbitmq.consumer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.NotificationMessage;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Ba service độc lập cùng nhận một thông báo từ fanout exchange,
 * mỗi service có queue riêng nên nhận một bản sao riêng.
 */
@Component
public class NotificationConsumer {

    private final DemoEventPublisher events;

    public NotificationConsumer(DemoEventPublisher events) {
        this.events = events;
    }

    @RabbitListener(queues = RabbitMQConfig.EMAIL_QUEUE)
    public void sendEmail(NotificationMessage notification) {
        events.publish(DemoEventType.NOTIFICATION_RECEIVED, "email-service", notification.id(),
                "Gửi email: " + notification.content());
    }

    @RabbitListener(queues = RabbitMQConfig.SMS_QUEUE)
    public void sendSms(NotificationMessage notification) {
        events.publish(DemoEventType.NOTIFICATION_RECEIVED, "sms-service", notification.id(),
                "Gửi SMS: " + notification.content());
    }

    @RabbitListener(queues = RabbitMQConfig.AUDIT_QUEUE)
    public void writeAuditLog(NotificationMessage notification) {
        events.publish(DemoEventType.NOTIFICATION_RECEIVED, "audit-service", notification.id(),
                "Ghi audit log: " + notification.content());
    }
}
