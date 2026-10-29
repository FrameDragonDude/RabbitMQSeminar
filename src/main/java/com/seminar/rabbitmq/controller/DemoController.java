package com.seminar.rabbitmq.controller;

import com.seminar.rabbitmq.consumer.OrderConsumer;
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

    @Autowired
    private OrderConsumer consumer;

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

    @Operation(summary = "4. Bắn liên tiếp nhiều đơn hàng (Test dồn hàng đợi)", 
               description = "Gửi dồn dập N đơn hàng vào queue để quan sát số lượng tin tồn đọng và tốc độ tiêu thụ trên Dashboard.")
    @GetMapping("/bulk-orders")
    public String sendBulkOrders(@RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            String code = "BULK_" + (100 + i);
            producer.sendOrder(new OrderMessage(code, 50000.0, false));
        }
        return "Đã bắn dồn dập " + count + " đơn hàng vào queue! Hãy nhìn RabbitMQ Dashboard để thấy hàng đợi xếp hàng.";
    }

    @Operation(summary = "5. Tạm dừng Consumer (Giả lập Service bị chết)", 
               description = "Khi tạm dừng, các đơn hàng bắn vào sẽ bị dồn ứ lại trong queue để người thuyết minh tha hồ giải thích.")
    @PostMapping("/pause-consumer")
    public String pauseConsumer() {
        consumer.setPaused(true);
        return "ĐÃ TẠM DỪNG CONSUMER! Bây giờ bạn bắn bao nhiêu đơn vào thì tin nhắn cũng sẽ NẰM YÊN trong hàng đợi để bạn chỉ tay giải thích.";
    }

    @Operation(summary = "6. Bật lại Consumer (Service sống lại và giải phóng tin)", 
               description = "Khởi động lại Consumer để bắt đầu lấy các tin đang dồn ứ ra xử lý dần.")
    @PostMapping("/resume-consumer")
    public String resumeConsumer() {
        consumer.setPaused(false);
        return "ĐÃ BẬT LẠI CONSUMER! Hệ thống đang bắt đầu rút dần các đơn hàng tồn đọng trong queue.";
    }

    @GetMapping("/consumer-status")
    public boolean getConsumerStatus() {
        return consumer.isPaused();
    }
}
