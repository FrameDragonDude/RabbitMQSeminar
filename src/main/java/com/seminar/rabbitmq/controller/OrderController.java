package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.consumer.OrderConsumer;
import com.seminar.rabbitmq.dto.OrderMessage;
import com.seminar.rabbitmq.dto.OrderRequest;
import com.seminar.rabbitmq.dto.SendResult;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import com.seminar.rabbitmq.producer.OrderProducer;
import com.seminar.rabbitmq.service.ProcessedOrderStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/orders")
@Tag(name = "Orders", description = "Direct exchange, Retry + DLQ, chống trùng và test tải")
public class OrderController {

    private static final int MAX_BULK = 5000;
    private static final double DEFAULT_AMOUNT = 45000;

    private final OrderProducer producer;
    private final ProcessedOrderStore processedOrders;
    private final OrderConsumer orderConsumer;
    private final DemoEventPublisher events;
    private final AtomicInteger bulkBatch = new AtomicInteger();

    public OrderController(OrderProducer producer, ProcessedOrderStore processedOrders,
                           OrderConsumer orderConsumer, DemoEventPublisher events) {
        this.producer = producer;
        this.processedOrders = processedOrders;
        this.orderConsumer = orderConsumer;
        this.events = events;
    }

    @Operation(summary = "Gửi một đơn hàng qua direct exchange",
            description = "simulateError=true: consumer ném exception, retry đủ số lần rồi tin chuyển sang order.dlq. "
                    + "Gửi lại cùng orderCode để thấy consumer bỏ qua đơn trùng.")
    @PostMapping
    public SendResult sendOrder(@RequestBody OrderRequest request) {
        if (request.orderCode() == null || request.orderCode().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "orderCode là bắt buộc");
        }
        String code = request.orderCode().trim();
        double amount = request.amount() == null ? DEFAULT_AMOUNT : request.amount();

        producer.sendOrder(new OrderMessage(code, amount, request.simulateError()));
        events.publish(DemoEventType.ORDER_SENT, "producer", code,
                "Publish → " + RabbitMQConfig.DIRECT_EXCHANGE + " [" + RabbitMQConfig.ORDER_ROUTING_KEY + "]"
                        + (request.simulateError() ? " (giả lập lỗi)" : ""));
        return SendResult.tracked(code, "Đã publish đơn " + code + ": "
                + RabbitMQConfig.DIRECT_EXCHANGE + " --[" + RabbitMQConfig.ORDER_ROUTING_KEY + "]--> " + RabbitMQConfig.ORDER_QUEUE);
    }

    @Operation(summary = "Gửi liên tiếp N đơn hàng (test san tải)",
            description = "Mỗi lần gọi tạo một lô mã đơn mới BULK-<lô>-<số thứ tự> nên không bị chặn trùng.")
    @PostMapping("/bulk")
    public SendResult sendBulk(
            @Parameter(description = "Số đơn, từ 1 đến 5000", example = "100")
            @RequestParam(defaultValue = "100") int count) {
        if (count < 1 || count > MAX_BULK) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "count phải từ 1 đến " + MAX_BULK);
        }
        int batch = bulkBatch.incrementAndGet();
        for (int i = 1; i <= count; i++) {
            String code = "%s%d-%04d".formatted(OrderMessage.BULK_PREFIX, batch, i);
            producer.sendOrder(new OrderMessage(code, DEFAULT_AMOUNT, false));
        }
        String reference = OrderMessage.BULK_PREFIX + batch;
        events.publish(DemoEventType.BULK_SENT, "producer", reference,
                "Publish " + count + " đơn → " + RabbitMQConfig.ORDER_QUEUE);
        return new SendResult(reference, "Đã publish " + count + " đơn, mã từ " + reference + "-0001 đến "
                + "%s-%04d".formatted(reference, count), "/api/tracking/" + reference + "-0001");
    }

    @Operation(summary = "Xoá bộ nhớ chống trùng", description = "Dùng khi muốn chạy lại demo từ đầu.")
    @DeleteMapping("/processed")
    public SendResult resetProcessed() {
        processedOrders.clear();
        orderConsumer.resetDuplicateCount();
        return new SendResult(null, "Đã xoá danh sách mã đơn đã xử lý");
    }
}
