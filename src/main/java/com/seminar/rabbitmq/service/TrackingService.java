package com.seminar.rabbitmq.service;

import com.seminar.rabbitmq.dto.TrackingResult;
import com.seminar.rabbitmq.event.DemoEvent;
import com.seminar.rabbitmq.event.DemoEventType;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.seminar.rabbitmq.event.DemoEventType.*;

/**
 * Lưu lịch sử sự kiện theo mã đơn / mã thông báo để tra cứu hành trình (dùng cho Postman).
 * Chỉ giữ {@value #MAX_REFERENCES} mã gần nhất, mỗi mã tối đa {@value #MAX_EVENTS} sự kiện.
 */
@Service
public class TrackingService {

    private static final int MAX_REFERENCES = 2000;
    private static final int MAX_EVENTS = 50;
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    // LinkedHashMap theo thứ tự truy cập: tự bỏ mã cũ nhất khi vượt giới hạn
    private final Map<String, List<DemoEvent>> history = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<DemoEvent>> eldest) {
            return size() > MAX_REFERENCES;
        }
    };

    public synchronized void record(DemoEvent event) {
        if (event.orderCode() == null) {
            return;
        }
        List<DemoEvent> events = history.computeIfAbsent(event.orderCode(), k -> new ArrayList<>());
        if (events.size() < MAX_EVENTS) {
            events.add(event);
        }
    }

    public synchronized Optional<TrackingResult> track(String reference) {
        List<DemoEvent> events = history.get(reference);
        return events == null ? Optional.empty() : Optional.of(toResult(reference, List.copyOf(events)));
    }

    public synchronized void clear() {
        history.clear();
    }

    private TrackingResult toResult(String reference, List<DemoEvent> events) {
        int attempts = count(events, PROCESSING);
        int processed = count(events, SUCCESS);
        int duplicates = count(events, DUPLICATE_SKIPPED);
        List<String> receivedBy = events.stream()
                .filter(e -> e.type() == NOTIFICATION_RECEIVED)
                .map(DemoEvent::source)
                .toList();
        DemoEventType last = events.get(events.size() - 1).type();

        String status;
        String summary;
        if (processed > 0) {
            status = "DONE";
            summary = "Đã xử lý thành công " + processed + " lần"
                    + (duplicates > 0 ? ", " + duplicates + " tin trùng bị bỏ qua" : "");
        } else if (has(events, DEAD_LETTERED) || has(events, INVALID_MESSAGE)) {
            status = "DEAD_LETTERED";
            summary = "Thất bại sau " + attempts + " lần xử lý, tin đã nằm trong order.dlq";
        } else if (last == FAILED || last == IN_PROGRESS) {
            status = "RETRYING";
            summary = "Đang retry, đã xử lý " + attempts + " lần";
        } else if (last == PROCESSING) {
            status = "PROCESSING";
            summary = "Consumer đang xử lý (lần " + attempts + ")";
        } else if (has(events, BROADCAST_SENT)) {
            status = "BROADCAST";
            summary = receivedBy.size() + " service đã nhận bản sao: " + String.join(", ", receivedBy);
        } else {
            status = "QUEUED";
            summary = "Đã vào queue, chờ consumer";
        }

        List<TrackingResult.Step> steps = events.stream()
                .map(e -> new TrackingResult.Step(TIME.format(Instant.ofEpochMilli(e.timestamp())),
                        e.type(), e.source(), e.message()))
                .toList();
        return new TrackingResult(reference, status, summary, attempts, processed, duplicates, receivedBy, steps);
    }

    private static int count(List<DemoEvent> events, DemoEventType type) {
        return (int) events.stream().filter(e -> e.type() == type).count();
    }

    private static boolean has(List<DemoEvent> events, DemoEventType type) {
        return events.stream().anyMatch(e -> e.type() == type);
    }
}
