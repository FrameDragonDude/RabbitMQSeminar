package com.seminar.rabbitmq.dto;

import io.swagger.v3.oas.annotations.media.Schema;

public record BroadcastRequest(
        @Schema(description = "Nội dung thông báo", example = "Hệ thống bảo trì lúc 00:00")
        String message) {
}
