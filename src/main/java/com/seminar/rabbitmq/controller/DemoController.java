package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.dto.OrderMessage;
import com.seminar.rabbitmq.producer.OrderProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@Tag(name = "RabbitMQ Demo API", description = "Các API thử nghiệm các kịch bản Direct, Fanout và Dead Letter Queue (DLQ)")
public class DemoController {

    @Autowired
    private OrderProducer producer;

    @Operation(summary = "1 & 2. Gửi đơn hàng (Direct Exchange & DLQ)", 
               description = "Gửi tin qua Direct Exchange. Nếu error=false sẽ xử lý thành công. Nếu error=true sẽ giả lập exception, retry 3 lần và chuyển vào DLQ.")
    @GetMapping("/send-order")
    public String sendOrder(
            @Parameter(description = "Mã đơn hàng", example = "ORD_001")
            @RequestParam String code, 
            @Parameter(description = "Bật giả lập lỗi Consumer để test DLQ", example = "false")
            @RequestParam(defaultValue = "false") boolean error) {
        OrderMessage msg = new OrderMessage(code, 45000.0, error);
        producer.sendOrder(msg);
        return "Đã gửi đơn: " + code + (error ? " [GIẢ LẬP LỖI -> SẼ RETRY & CHUYỂN DLQ]" : " [THÀNH CÔNG -> CONSUMER NHẬN NGAY]");
    }

    @Operation(summary = "3. Gửi thông báo phát thanh (Fanout Exchange)", 
               description = "Gửi tin broadcast qua Fanout Exchange đến tất cả các Queue liên kết mà không cần routing key.")
    @GetMapping("/broadcast")
    public String broadcast(
            @Parameter(description = "Nội dung thông báo broadcast", example = "Hệ thống bảo trì lúc 00:00")
            @RequestParam(defaultValue = "Hệ thống bảo trì lúc 00:00") String msg) {
        producer.sendBroadcast(msg);
        return "Đã gửi broadcast: " + msg;
    }
}
