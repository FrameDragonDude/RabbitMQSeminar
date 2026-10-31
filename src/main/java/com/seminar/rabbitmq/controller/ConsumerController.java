package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.dto.SendResult;
import com.seminar.rabbitmq.service.ConsumerControlService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consumer")
@Tag(name = "Consumer", description = "Giả lập service xử lý đơn bị sập và phục hồi")
public class ConsumerController {

    private final ConsumerControlService consumerControl;

    public ConsumerController(ConsumerControlService consumerControl) {
        this.consumerControl = consumerControl;
    }

    @Operation(summary = "Dừng consumer của order.queue",
            description = "Consumer huỷ subscribe: tin mới nằm ở Ready, cột Consumers của order.queue về 0.")
    @PostMapping("/stop")
    public SendResult stop() {
        consumerControl.stop();
        return new SendResult(null, "Consumer đã dừng");
    }

    @Operation(summary = "Khởi động lại consumer của order.queue")
    @PostMapping("/start")
    public SendResult start() {
        consumerControl.start();
        return new SendResult(null, "Consumer đang chạy");
    }
}
