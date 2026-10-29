package com.seminar.rabbitmq.consumer;

import com.seminar.rabbitmq.config.RabbitMQConfig;
import com.seminar.rabbitmq.dto.OrderMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    // Cờ kiểm soát tạm dừng Consumer để demo hàng đợi dồn tin
    private volatile boolean isPaused = false;

    public void setPaused(boolean paused) {
        this.isPaused = paused;
    }

    public boolean isPaused() {
        return isPaused;
    }

    @RabbitListener(queues = RabbitMQConfig.ORDER_QUEUE)
    public void consumeOrder(OrderMessage message) throws InterruptedException {
        // Nếu đang ở trạng thái TẠM DỪNG (mô phỏng service chết), giữ tin chờ trong queue
        while (isPaused) {
            Thread.sleep(1000);
        }

        // Nếu là đơn test dồn tải hàng loạt (BULK_), xử lý nhanh 100ms mỗi đơn để thấy số đếm lùi mượt mà
        // Nếu là đơn lẻ bình thường (ORD_), xử lý 2 giây để quan sát Dashboard
        if (message.getOrderCode() != null && message.getOrderCode().startsWith("BULK_")) {
            Thread.sleep(100);
        } else {
            Thread.sleep(2000);
        }

        if (message.isSimulateError()) {
            System.err.println("===> [LỖI NGHIỆP VỤ] Đơn hàng " + message.getOrderCode() + " bị lỗi logic! Ném exception để kích hoạt Retry...");
            throw new RuntimeException("Lỗi xử lý đơn hàng: " + message.getOrderCode());
        }
        System.out.println("===> [CONSUMER] ĐÃ XỬ LÝ THÀNH CÔNG đơn hàng: " + message.getOrderCode() + " (Đã gửi ACK)");
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
