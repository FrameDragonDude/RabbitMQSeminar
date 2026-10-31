package com.seminar.rabbitmq.config;

import com.seminar.rabbitmq.consumer.OrderInProgressException;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;

/**
 * Chạy khi đã hết retry (hoặc lỗi không cần retry). Mặc định giữ hành vi của Spring Boot
 * (reject, không requeue → DLQ) và ghi thêm một sự kiện để giao diện hiển thị.
 */
public class DeadLetterRecoverer extends RejectAndDontRequeueRecoverer {

    private final DemoEventPublisher events;

    public DeadLetterRecoverer(DemoEventPublisher events) {
        this.events = events;
    }

    @Override
    public void recover(Message message, Throwable cause) {
        String queue = message.getMessageProperties().getConsumerQueue();
        String orderCode = message.getMessageProperties().getMessageId();

        // Tin không lỗi, chỉ là luồng khác vẫn đang giữ mã đơn: trả lại queue để xử lý sau, không đưa vào DLQ
        if (hasCause(cause, OrderInProgressException.class)) {
            events.publish(DemoEventType.IN_PROGRESS, "broker", orderCode, "Trả tin về " + queue + " (requeue)");
            throw new ImmediateRequeueAmqpException("Đơn " + orderCode + " đang được xử lý, requeue", cause);
        }

        String target = RabbitMQConfig.ORDER_QUEUE.equals(queue)
                ? RabbitMQConfig.DLX_EXCHANGE + " → " + RabbitMQConfig.DLQ_QUEUE
                : "bị loại bỏ (" + queue + " không cấu hình DLX)";

        if (isConversionError(cause)) {
            events.publish(DemoEventType.INVALID_MESSAGE, "broker", orderCode,
                    "Không đọc được tin (content-type " + message.getMessageProperties().getContentType()
                            + "), không retry → " + target);
        } else {
            events.publish(DemoEventType.DEAD_LETTERED, "broker", orderCode,
                    "Hết retry → reject(requeue=false) → " + target);
        }
        super.recover(message, cause);
    }

    private static boolean isConversionError(Throwable error) {
        return hasCause(error, org.springframework.amqp.support.converter.MessageConversionException.class)
                || hasCause(error, org.springframework.messaging.converter.MessageConversionException.class);
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
