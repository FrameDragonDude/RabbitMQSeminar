package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.consumer.OrderConsumer;
import com.seminar.rabbitmq.dto.DemoStats;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.service.ConsumerControlService;
import com.seminar.rabbitmq.service.ProcessedOrderStore;
import com.seminar.rabbitmq.service.QueueStatsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/monitor")
@Tag(name = "Monitor", description = "Số liệu và sự kiện thời gian thực cho giao diện demo")
public class MonitorController {

    private final QueueStatsService queueStats;
    private final ConsumerControlService consumerControl;
    private final ProcessedOrderStore processedOrders;
    private final OrderConsumer orderConsumer;
    private final DemoEventPublisher events;

    public MonitorController(QueueStatsService queueStats, ConsumerControlService consumerControl,
                             ProcessedOrderStore processedOrders, OrderConsumer orderConsumer,
                             DemoEventPublisher events) {
        this.queueStats = queueStats;
        this.consumerControl = consumerControl;
        this.processedOrders = processedOrders;
        this.orderConsumer = orderConsumer;
        this.events = events;
    }

    @Operation(summary = "Trạng thái consumer, bộ đếm và số tin trong từng queue")
    @GetMapping("/stats")
    public DemoStats stats() {
        return new DemoStats(consumerControl.isRunning(), processedOrders.processedCount(),
                orderConsumer.duplicateCount(), queueStats.queueStats(), queueStats.legacyQueueReady());
    }

    @Operation(summary = "Luồng sự kiện (Server-Sent Events)")
    @GetMapping(path = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events() {
        return events.subscribe();
    }
}
