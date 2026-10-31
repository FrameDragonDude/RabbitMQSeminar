package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.BroadcastRequest;
import com.seminar.rabbitmq.dto.NotificationMessage;
import com.seminar.rabbitmq.dto.SendResult;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import com.seminar.rabbitmq.producer.OrderProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/notifications")
@Tag(name = "Notifications", description = "Fanout exchange")
public class NotificationController {

    private final OrderProducer producer;
    private final DemoEventPublisher events;
    private final AtomicInteger sequence = new AtomicInteger();

    public NotificationController(OrderProducer producer, DemoEventPublisher events) {
        this.producer = producer;
        this.events = events;
    }

    @Operation(summary = "Phát thông báo qua fanout exchange",
            description = "Một message được sao chép tới cả 3 queue email, sms, audit. "
                    + "Dùng trackingUrl trong kết quả để xem service nào đã nhận.")
    @PostMapping("/broadcast")
    public SendResult broadcast(@RequestBody BroadcastRequest request) {
        if (request.message() == null || request.message().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "message là bắt buộc");
        }
        String id = "NTF-" + sequence.incrementAndGet();
        producer.broadcast(new NotificationMessage(id, request.message()));
        events.publish(DemoEventType.BROADCAST_SENT, "producer", id,
                "Publish → " + RabbitMQConfig.FANOUT_EXCHANGE + ": " + request.message());
        return SendResult.tracked(id, "Đã phát thông báo tới " + RabbitMQConfig.FANOUT_EXCHANGE);
    }
}
