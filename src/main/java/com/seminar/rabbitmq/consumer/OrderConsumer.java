package com.seminar.rabbitmq.consumer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.OrderMessage;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import com.seminar.rabbitmq.service.ProcessedOrderStore;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class OrderConsumer {

    /** Id của listener container, dùng để dừng/bật lại trong {@code ConsumerControlService}. */
    public static final String LISTENER_ID = "orderListener";

    private static final String SOURCE = "order-consumer";

    private final ProcessedOrderStore processedOrders;
    private final DemoEventPublisher events;
    private final long processingMs;
    private final long bulkProcessingMs;
    private final int maxAttempts;
    private final AtomicLong duplicateCount = new AtomicLong();

    public OrderConsumer(ProcessedOrderStore processedOrders,
                         DemoEventPublisher events,
                         @Value("${demo.processing.order-ms:2000}") long processingMs,
                         @Value("${demo.processing.bulk-ms:100}") long bulkProcessingMs,
                         @Value("${spring.rabbitmq.listener.simple.retry.max-attempts:3}") int maxAttempts) {
        this.processedOrders = processedOrders;
        this.events = events;
        this.processingMs = processingMs;
        this.bulkProcessingMs = bulkProcessingMs;
        this.maxAttempts = maxAttempts;
    }

    /**
     * ACK tự động khi method kết thúc bình thường. Nếu ném exception, Spring Retry gọi lại method
     * (tổng cộng {@code max-attempts} lần); hết lượt thì {@code DeadLetterRecoverer} reject tin sang DLQ.
     */
    @RabbitListener(id = LISTENER_ID, queues = RabbitMQConfig.ORDER_QUEUE)
    public void consumeOrder(OrderMessage order) throws InterruptedException {
        String code = order.getOrderCode();
        if (code == null || code.isBlank()) {
            throw new AmqpRejectAndDontRequeueException("Tin không có orderCode");
        }

        int attempt = currentAttempt();

        // Idempotent consumer: mã đơn đã xử lý thì bỏ qua, method kết thúc bình thường nên tin vẫn được ACK
        switch (processedOrders.tryStart(code)) {
            case DONE -> {
                duplicateCount.incrementAndGet();
                events.publish(DemoEventType.DUPLICATE_SKIPPED, SOURCE, code,
                        "Mã đơn đã được xử lý trước đó → bỏ qua, ACK để xoá khỏi queue");
                return;
            }
            case IN_PROGRESS -> {
                events.publish(DemoEventType.IN_PROGRESS, SOURCE, code,
                        "Đang được xử lý ở luồng khác → chờ rồi thử lại (lần " + attempt + "/" + maxAttempts + ")");
                throw new OrderInProgressException(code);
            }
            case ACQUIRED -> {
                // được quyền xử lý
            }
        }

        boolean bulk = code.startsWith(OrderMessage.BULK_PREFIX);
        try {
            if (!bulk) {
                events.publish(DemoEventType.PROCESSING, SOURCE, code,
                        "Nhận đơn, đang xử lý (lần " + attempt + "/" + maxAttempts + ")");
            }
            Thread.sleep(bulk ? bulkProcessingMs : processingMs);

            if (order.isSimulateError()) {
                throw new IllegalStateException("Lỗi nghiệp vụ giả lập");
            }

            processedOrders.markDone(code);
            events.publish(DemoEventType.SUCCESS, SOURCE, code, "Xử lý xong → ACK");
        } catch (Exception e) {
            processedOrders.release(code);
            String next = attempt < maxAttempts ? " → retry" : " → hết lượt retry";
            events.publish(DemoEventType.FAILED, SOURCE, code,
                    "Lần " + attempt + "/" + maxAttempts + " thất bại: " + e.getMessage() + next);
            throw e;
        }
    }

    public long duplicateCount() {
        return duplicateCount.get();
    }

    public void resetDuplicateCount() {
        duplicateCount.set(0);
    }

    /** Lần thử hiện tại (bắt đầu từ 1), lấy từ context của Spring Retry. */
    private static int currentAttempt() {
        RetryContext context = RetrySynchronizationManager.getContext();
        return context == null ? 1 : context.getRetryCount() + 1;
    }
}
