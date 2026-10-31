package com.seminar.rabbitmq.dto;

import io.swagger.v3.oas.annotations.media.Schema;

public record OrderRequest(
        @Schema(description = "Mã đơn hàng, cũng là khoá chống trùng", example = "ORD-0001")
        String orderCode,
        @Schema(description = "Giá trị đơn", example = "45000")
        Double amount,
        @Schema(description = "Giả lập lỗi ở consumer để test Retry và DLQ", example = "false")
        boolean simulateError) {
}
