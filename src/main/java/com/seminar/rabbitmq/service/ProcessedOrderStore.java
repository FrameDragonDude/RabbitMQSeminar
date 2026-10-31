package com.seminar.rabbitmq.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kho chống trùng (idempotency) theo orderCode cho consumer.
 *
 * <p>RabbitMQ đảm bảo at-least-once: cùng một đơn có thể đến consumer nhiều lần
 * (producer gửi lại, consumer chết trước khi ACK nên broker giao lại...).
 * Consumer dùng kho này để mỗi mã đơn chỉ được xử lý thành công đúng một lần.
 *
 * <p>Bản demo lưu trong bộ nhớ nên mất khi khởi động lại. Thực tế nên dùng
 * bảng DB có unique constraint trên orderCode (cùng transaction với nghiệp vụ)
 * hoặc Redis {@code SET key NX EX ...}.
 */
@Component
public class ProcessedOrderStore {

    private enum State { PROCESSING, DONE }

    /** Kết quả khi consumer xin quyền xử lý một mã đơn. */
    public enum Claim {
        /** Được quyền xử lý. */
        ACQUIRED,
        /** Một luồng khác đang xử lý dở mã đơn này (vd. bản giao lại sau khi consumer bị dừng). */
        IN_PROGRESS,
        /** Đã xử lý thành công trước đó: tin trùng. */
        DONE
    }

    private final ConcurrentMap<String, State> states = new ConcurrentHashMap<>();
    private final AtomicLong processedCount = new AtomicLong();

    /**
     * Giữ chỗ để xử lý một đơn. Thao tác nguyên tử nên an toàn khi có nhiều consumer song song.
     */
    public Claim tryStart(String orderCode) {
        State previous = states.putIfAbsent(orderCode, State.PROCESSING);
        if (previous == null) {
            return Claim.ACQUIRED;
        }
        return previous == State.DONE ? Claim.DONE : Claim.IN_PROGRESS;
    }

    public void markDone(String orderCode) {
        if (states.put(orderCode, State.DONE) != State.DONE) {
            processedCount.incrementAndGet();
        }
    }

    /** Xử lý thất bại: nhả chỗ để lần retry (hoặc lần replay từ DLQ) được xử lý lại. */
    public void release(String orderCode) {
        states.remove(orderCode, State.PROCESSING);
    }

    public long processedCount() {
        return processedCount.get();
    }

    public void clear() {
        states.clear();
        processedCount.set(0);
    }
}
