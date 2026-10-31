package com.seminar.rabbitmq.dto;

import com.seminar.rabbitmq.event.DemoEventType;

import java.util.List;

/**
 * Hành trình của một đơn hàng / thông báo qua hệ thống.
 *
 * @param reference        mã đơn hoặc mã thông báo
 * @param status           QUEUED, PROCESSING, RETRYING, DONE, DEAD_LETTERED hoặc BROADCAST
 * @param summary          tóm tắt dễ đọc
 * @param attempts         số lần consumer đã xử lý
 * @param processedTimes   số lần xử lý thành công (đúng ra luôn ≤ 1 nhờ chống trùng)
 * @param duplicatesSkipped số tin trùng bị bỏ qua
 * @param receivedBy       các service đã nhận (với thông báo fanout)
 * @param events           các sự kiện theo thứ tự thời gian
 */
public record TrackingResult(String reference, String status, String summary,
                             int attempts, int processedTimes, int duplicatesSkipped,
                             List<String> receivedBy, List<Step> events) {

    public record Step(String time, DemoEventType type, String source, String message) {
    }
}
