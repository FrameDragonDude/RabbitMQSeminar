package com.seminar.rabbitmq.event;

import com.seminar.rabbitmq.service.TrackingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Ghi log, lưu lịch sử theo mã (cho /api/tracking) và đẩy sự kiện tới mọi trình duyệt
 * đang mở trang demo (Server-Sent Events).
 */
@Component
public class DemoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DemoEventPublisher.class);

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final TrackingService tracking;

    public DemoEventPublisher(TrackingService tracking) {
        this.tracking = tracking;
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L); // 0 = không timeout
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        try {
            // Gửi ngay một comment để response được flush, trình duyệt báo "đã kết nối" mà không phải chờ sự kiện đầu tiên
            emitter.send(SseEmitter.event().comment("connected"));
        } catch (IOException e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    public void publish(DemoEventType type, String source, String orderCode, String message) {
        DemoEvent event = new DemoEvent(System.currentTimeMillis(), type, source, orderCode, message);
        tracking.record(event);

        if (type == DemoEventType.FAILED || type == DemoEventType.DEAD_LETTERED || type == DemoEventType.INVALID_MESSAGE) {
            log.warn("[{}] {} {} - {}", source, type, orderCode, message);
        } else {
            log.info("[{}] {} {} - {}", source, type, orderCode, message);
        }

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("demo").data(event));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
            }
        }
    }
}
