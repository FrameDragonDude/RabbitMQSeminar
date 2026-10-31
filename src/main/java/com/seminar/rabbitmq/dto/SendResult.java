package com.seminar.rabbitmq.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Kết quả trả về của các API gửi message.
 *
 * @param reference   mã đơn / mã lô / mã thông báo liên quan
 * @param message     mô tả kết quả
 * @param trackingUrl đường dẫn xem hành trình của message (nếu có)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SendResult(String reference, String message, String trackingUrl) {

    public SendResult(String reference, String message) {
        this(reference, message, null);
    }

    public static SendResult tracked(String reference, String message) {
        return new SendResult(reference, message, "/api/tracking/" + reference);
    }
}
