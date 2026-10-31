package com.seminar.rabbitmq.dto;

/**
 * Trạng thái một queue cùng exchange/binding đứng trước nó.
 *
 * @param ready     số tin đang chờ (Ready) trên broker
 * @param consumers số consumer đang subscribe queue
 */
public record QueueStat(String queue, String exchange, String exchangeType, String routingKey,
                        int ready, int consumers) {
}
