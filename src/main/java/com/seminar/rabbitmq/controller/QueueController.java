package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.dto.SendResult;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Set;

import static com.seminar.rabbitmq.config.RabbitMQConfig.*;

@RestController
@RequestMapping("/api/queues")
@Tag(name = "Queues", description = "Dọn dữ liệu demo trên broker")
public class QueueController {

    private static final Set<String> PURGEABLE = Set.of(ORDER_QUEUE, DLQ_QUEUE, EMAIL_QUEUE, SMS_QUEUE, AUDIT_QUEUE);

    private final AmqpAdmin amqpAdmin;
    private final DemoEventPublisher events;

    public QueueController(AmqpAdmin amqpAdmin, DemoEventPublisher events) {
        this.amqpAdmin = amqpAdmin;
        this.events = events;
    }

    @Operation(summary = "Xoá toàn bộ tin đang chờ (Ready) trong một queue của demo")
    @PostMapping("/{name}/purge")
    public SendResult purge(@PathVariable String name) {
        if (!PURGEABLE.contains(name)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Không hỗ trợ dọn queue " + name);
        }
        int count = amqpAdmin.purgeQueue(name);
        events.publish(DemoEventType.QUEUE_PURGED, "admin", null, "Đã xoá " + count + " tin trong " + name);
        return new SendResult(name, "Đã xoá " + count + " tin");
    }

    @Operation(summary = "Xoá queue notification.queue của phiên bản cũ")
    @DeleteMapping("/legacy")
    public SendResult deleteLegacyQueue() {
        amqpAdmin.deleteQueue(LEGACY_NOTIFICATION_QUEUE);
        events.publish(DemoEventType.QUEUE_PURGED, "admin", null, "Đã xoá queue cũ " + LEGACY_NOTIFICATION_QUEUE);
        return new SendResult(LEGACY_NOTIFICATION_QUEUE, "Đã xoá queue cũ");
    }
}
