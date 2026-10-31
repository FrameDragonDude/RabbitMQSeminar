package com.seminar.rabbitmq.consumer;

/**
 * Mã đơn đang được một luồng khác xử lý dở. Tin này chưa chắc là trùng (luồng kia có thể thất bại),
 * nên không được ACK bỏ qua: để retry, hết lượt thì trả lại queue thay vì đẩy sang DLQ.
 */
public class OrderInProgressException extends RuntimeException {

    public OrderInProgressException(String orderCode) {
        super("Đơn " + orderCode + " đang được xử lý ở luồng khác");
    }
}
