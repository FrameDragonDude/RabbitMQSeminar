package com.seminar.rabbitmq.dto;

/**
 * Payload thông báo gửi qua fanout exchange (serialize dạng JSON).
 *
 * @param id      mã thông báo, dùng để tra cứu hành trình qua /api/tracking/{id}
 * @param content nội dung
 */
public record NotificationMessage(String id, String content) {
}
