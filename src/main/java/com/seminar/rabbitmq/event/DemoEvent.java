package com.seminar.rabbitmq.event;

/**
 * Một sự kiện thật xảy ra ở producer/consumer/broker, được đẩy lên giao diện qua SSE.
 *
 * @param timestamp thời điểm (epoch millis)
 * @param type      loại sự kiện
 * @param source    thành phần phát sinh: producer, order-consumer, email-service...
 * @param orderCode mã đơn liên quan (có thể null)
 * @param message   mô tả ngắn
 */
public record DemoEvent(long timestamp, DemoEventType type, String source, String orderCode, String message) {
}
