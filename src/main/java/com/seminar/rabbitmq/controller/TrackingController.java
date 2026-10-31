package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.dto.TrackingResult;
import com.seminar.rabbitmq.service.TrackingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/tracking")
@Tag(name = "Tracking", description = "Xem hành trình của một đơn hàng / thông báo")
public class TrackingController {

    private final TrackingService tracking;

    public TrackingController(TrackingService tracking) {
        this.tracking = tracking;
    }

    @Operation(summary = "Hành trình của một mã đơn (ORD-..., ERR-..., BULK-...) hoặc mã thông báo (NTF-...)",
            description = "Trạng thái hiện tại và toàn bộ sự kiện: publish → xử lý → retry → DLQ / ACK / bỏ qua tin trùng.")
    @GetMapping("/{reference}")
    public TrackingResult track(@PathVariable String reference) {
        return tracking.track(reference)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Chưa có sự kiện nào cho " + reference));
    }
}
