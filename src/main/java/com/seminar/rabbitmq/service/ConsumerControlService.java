package com.seminar.rabbitmq.service;

import com.seminar.rabbitmq.consumer.OrderConsumer;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import com.seminar.rabbitmq.event.DemoEventType;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.stereotype.Service;

/**
 * Bật/tắt listener container của order.queue để giả lập service xử lý đơn bị sập.
 *
 * <p>Khi container dừng, consumer huỷ subscribe khỏi broker: tin mới nằm ở trạng thái
 * Ready và số consumer của queue về 0, đúng như khi service chết thật.
 */
@Service
public class ConsumerControlService {

    private final RabbitListenerEndpointRegistry registry;
    private final DemoEventPublisher events;

    public ConsumerControlService(RabbitListenerEndpointRegistry registry, DemoEventPublisher events) {
        this.registry = registry;
        this.events = events;
    }

    public void stop() {
        MessageListenerContainer container = orderContainer();
        if (container.isRunning()) {
            container.stop(); // chờ tin đang xử lý dở hoàn tất rồi mới dừng
            events.publish(DemoEventType.CONSUMER_STOPPED, "order-consumer", null,
                    "Đã dừng listener, tin mới sẽ nằm lại ở trạng thái Ready");
        }
    }

    public void start() {
        MessageListenerContainer container = orderContainer();
        if (!container.isRunning()) {
            container.start();
            events.publish(DemoEventType.CONSUMER_STARTED, "order-consumer", null,
                    "Đã khởi động lại listener, bắt đầu xử lý tin tồn đọng");
        }
    }

    public boolean isRunning() {
        return orderContainer().isRunning();
    }

    private MessageListenerContainer orderContainer() {
        return registry.getListenerContainer(OrderConsumer.LISTENER_ID);
    }
}
