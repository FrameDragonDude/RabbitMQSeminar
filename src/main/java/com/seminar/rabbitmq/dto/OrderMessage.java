package com.seminar.rabbitmq.dto;

import java.io.Serializable;

public class OrderMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderCode;
    private double amount;
    private boolean simulateError;

    // Constructor không tham số
    public OrderMessage() {
    }

    // Constructor đầy đủ tham số
    public OrderMessage(String orderCode, double amount, boolean simulateError) {
        this.orderCode = orderCode;
        this.amount = amount;
        this.simulateError = simulateError;
    }

    // Getters and Setters
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