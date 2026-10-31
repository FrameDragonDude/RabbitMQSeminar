package com.seminar.rabbitmq.dto;

/**
 * Payload gửi qua RabbitMQ (serialize dạng JSON).
 */
public class OrderMessage {

    /** Tiền tố mã đơn của bài test tải hàng loạt: consumer xử lý nhanh hơn để thấy queue rút dần. */
    public static final String BULK_PREFIX = "BULK-";

    private String orderCode;
    private double amount;
    private boolean simulateError;

    public OrderMessage() {
    }

    public OrderMessage(String orderCode, double amount, boolean simulateError) {
        this.orderCode = orderCode;
        this.amount = amount;
        this.simulateError = simulateError;
    }

    public String getOrderCode() {
        return orderCode;
    }

    public void setOrderCode(String orderCode) {
        this.orderCode = orderCode;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public boolean isSimulateError() {
        return simulateError;
    }

    public void setSimulateError(boolean simulateError) {
        this.simulateError = simulateError;
    }
}
