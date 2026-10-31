package com.seminar.rabbitmq.dto;

import java.util.List;

/**
 * Số liệu tổng hợp cho giao diện demo.
 *
 * @param consumerRunning  listener của order.queue có đang chạy không
 * @param processedOrders  số đơn đã xử lý thành công (mỗi mã đơn tính 1 lần)
 * @param duplicateSkipped số lần consumer bỏ qua tin trùng mã đơn
 * @param legacyQueueReady số tin trong queue cũ notification.queue, {@code null} nếu đã xoá
 */
public record DemoStats(boolean consumerRunning, long processedOrders, long duplicateSkipped,
                        List<QueueStat> queues, Integer legacyQueueReady) {
}
