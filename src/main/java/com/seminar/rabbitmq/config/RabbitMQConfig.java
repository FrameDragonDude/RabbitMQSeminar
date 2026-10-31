package com.seminar.rabbitmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.seminar.rabbitmq.event.DemoEventPublisher;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitRetryTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.Map;

/**
 * Khai báo topology trên broker:
 *
 * <pre>
 * order.direct.exchange  --[order.created]--> order.queue  --(reject)--> order.dlx.exchange --[order.dead]--> order.dlq
 * system.fanout.exchange ------------------> notification.email.queue
 *                                         \-> notification.sms.queue
 *                                         \-> notification.audit.queue
 * </pre>
 */
@Configuration
public class RabbitMQConfig {

    // Exchange
    public static final String DIRECT_EXCHANGE = "order.direct.exchange";
    public static final String FANOUT_EXCHANGE = "system.fanout.exchange";
    public static final String DLX_EXCHANGE = "order.dlx.exchange";

    // Queue
    public static final String ORDER_QUEUE = "order.queue";
    public static final String DLQ_QUEUE = "order.dlq";
    public static final String EMAIL_QUEUE = "notification.email.queue";
    public static final String SMS_QUEUE = "notification.sms.queue";
    public static final String AUDIT_QUEUE = "notification.audit.queue";

    // Routing key
    public static final String ORDER_ROUTING_KEY = "order.created";
    public static final String DLQ_ROUTING_KEY = "order.dead";

    /** Queue của phiên bản demo cũ: không còn consumer nhưng vẫn bind với fanout exchange nên tin dồn lại. */
    public static final String LEGACY_NOTIFICATION_QUEUE = "notification.queue";

    /**
     * Gửi/nhận message dạng JSON thay cho Java serialization mặc định,
     * để mở "Get messages" trên Management UI vẫn đọc được nội dung.
     * Spring Boot tự gắn bean này cho cả RabbitTemplate và listener container.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * Không retry những lỗi mà thử lại cũng vô ích: tin sai định dạng (vd. tin Java serialization
     * của bản cũ) hoặc tin bị chủ động reject. Các lỗi này đi thẳng tới recoverer → DLQ.
     */
    @Bean
    public RabbitRetryTemplateCustomizer listenerRetryCustomizer(RabbitProperties properties) {
        int maxAttempts = properties.getListener().getSimple().getRetry().getMaxAttempts();
        return (target, retryTemplate) -> {
            if (target == RabbitRetryTemplateCustomizer.Target.LISTENER) {
                Map<Class<? extends Throwable>, Boolean> retryable = Map.of(
                        org.springframework.amqp.support.converter.MessageConversionException.class, false,
                        org.springframework.messaging.converter.MessageConversionException.class, false,
                        AmqpRejectAndDontRequeueException.class, false);
                // traverseCauses = true: lỗi thật nằm bên trong ListenerExecutionFailedException
                retryTemplate.setRetryPolicy(new SimpleRetryPolicy(maxAttempts, retryable, true, true));
            }
        };
    }

    /**
     * Được gọi khi đã hết số lần retry: reject(requeue=false) để broker chuyển tin sang DLX.
     */
    @Bean
    public MessageRecoverer deadLetterRecoverer(DemoEventPublisher events) {
        return new DeadLetterRecoverer(events);
    }

    // ---------- Direct exchange + Dead Letter ----------

    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(DIRECT_EXCHANGE);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DLX_EXCHANGE);
    }

    // Tin bị reject (requeue=false) sẽ được broker chuyển sang DLX với routing key order.dead
    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(ORDER_QUEUE)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLQ_ROUTING_KEY)
                .build();
    }

    // Không có consumer: tin lỗi nằm lại để kiểm tra trên Management UI
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }

    @Bean
    public Binding orderBinding() {
        return BindingBuilder.bind(orderQueue()).to(directExchange()).with(ORDER_ROUTING_KEY);
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with(DLQ_ROUTING_KEY);
    }

    // ---------- Fanout exchange: 1 message -> mọi queue được bind ----------

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE);
    }

    @Bean
    public Queue emailQueue() {
        return QueueBuilder.durable(EMAIL_QUEUE).build();
    }

    @Bean
    public Queue smsQueue() {
        return QueueBuilder.durable(SMS_QUEUE).build();
    }

    @Bean
    public Queue auditQueue() {
        return QueueBuilder.durable(AUDIT_QUEUE).build();
    }

    @Bean
    public Binding emailBinding() {
        return BindingBuilder.bind(emailQueue()).to(fanoutExchange());
    }

    @Bean
    public Binding smsBinding() {
        return BindingBuilder.bind(smsQueue()).to(fanoutExchange());
    }

    @Bean
    public Binding auditBinding() {
        return BindingBuilder.bind(auditQueue()).to(fanoutExchange());
    }
}
