package com.seminar.rabbitmq.consumer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.OrderMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @RabbitListener(queues = RabbitMQConfig.ORDER_QUEUE)
    public void consumeOrder(OrderMessage message) {
        System.out.println("===> [CONSUMER] Nhận đơn hàng: " + message.getOrderCode());
        if (message.isSimulateError()) {
            System.err.println("===> [LỖI NGHIỆP VỤ] Giả lập lỗi xử lý để chuyển DLQ!");
            throw new RuntimeException("Lỗi xử lý đơn hàng: " + message.getOrderCode());
        }
        System.out.println("===> [CONSUMER] Đã xử lý thành công đơn hàng: " + message.getOrderCode());
    }

    @RabbitListener(queues = RabbitMQConfig.NOTIFICATION_QUEUE)
    public void consumeNotification(String message) {
        System.out.println("===> [FANOUT CONSUMER] Nhận thông báo phát thanh: " + message);
    }

    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void consumeDLQ(OrderMessage message) {
        System.err.println("===> [DEAD LETTER QUEUE (DLQ)] Đã bắt gói tin lỗi: " + message.getOrderCode());
    }
}
