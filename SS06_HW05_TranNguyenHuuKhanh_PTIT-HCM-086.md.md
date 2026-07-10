# BÀI 5: Thiết kế Hệ thống Hướng Sự kiện và Xử lý Đơn hàng


## 1. Ý đồ thiết kế quy trình 3 bước

Quy trình 3 bước được thiết kế để AI giải bài toán từ kiến trúc đến xử lý lỗi và cuối cùng là sinh mã. Bước 1 giúp lựa chọn Message Broker phù hợp. Bước 2 kiểm tra khả năng chịu lỗi khi consumer bị sập và thiết kế idempotency để chống gửi trùng email. Bước 3 chuyển thiết kế thành mã Java Spring Boot cụ thể với Kafka Consumer, Redis, retry, DLQ và logging.

## 2. Prompt bước 1 - Tư vấn và lựa chọn Broker

```text
Bạn là System Designer cho hệ thống thương mại điện tử SpeedyCart.

Bối cảnh:
Sau khi đơn hàng hoàn tất, hệ thống cần phát sự kiện `OrderCompletedEvent` để các consumer độc lập xử lý: Email Service gửi email xác nhận, Loyalty Service cộng điểm, Warehouse Service nhận thông báo đóng gói. API đặt hàng cần phản hồi nhanh và không phụ thuộc trực tiếp vào các dịch vụ phụ.

Yêu cầu:
- Đề xuất 2 giải pháp Message Broker: Apache Kafka và RabbitMQ.
- Lập bảng so sánh chi tiết theo các tiêu chí: hiệu năng, scalability, độ tin cậy, mô hình message, khả năng replay event, độ phức tạp vận hành.
- Phân tích trade-off trong context SpeedyCart.
- Đưa ra khuyến nghị nên chọn broker nào cho `OrderCompletedEvent` và vì sao.

Format:
- Giải thích ngắn gọn từng broker.
- Bảng so sánh Markdown.
- Kết luận lựa chọn.
```

## 3. Prompt bước 2 - What-if Scenario và Idempotency

```text
Tiếp tục với kiến trúc Event-Driven của SpeedyCart.

Giả định sự cố:
Email Service bị sập nguồn trong 3 tiếng trong khi Order Service vẫn publish `OrderCompletedEvent` lên broker.

Hãy phân tích:
- Tin nhắn trên Kafka/RabbitMQ có bị mất không?
- Khi Email Service hoạt động trở lại, làm thế nào để nó tiếp tục tiêu thụ event chưa xử lý?
- Làm thế nào để đảm bảo mỗi đơn hàng chỉ gửi đúng 1 email xác nhận, tránh gửi trùng email cho khách hàng?
- Đề xuất giải pháp dùng Redis làm Idempotency Key Store.

Yêu cầu kỹ thuật:
- Mỗi event có `eventId` duy nhất.
- Trước khi gửi email, kiểm tra Redis key `processed:email:{eventId}`.
- Nếu key tồn tại thì bỏ qua event.
- Nếu chưa tồn tại thì gửi email, sau khi thành công mới set key với TTL.
- Phân tích rủi ro nếu set key trước khi gửi email.
- Đề xuất retry và DLQ nếu gửi mail thất bại.
```

## 4. Prompt bước 3 - Code Generation with CoT

```text
Bạn là Senior Java Spring Boot Developer.

Dựa trên thiết kế Event-Driven của SpeedyCart, hãy viết mã nguồn Java cho Kafka Consumer xử lý `OrderCompletedEvent`.

Hãy suy luận theo từng bước trước khi đưa code:
1. Consumer nhận event bằng `@KafkaListener`.
2. Kiểm tra idempotency bằng Redis Template với key `processed:email:{eventId}`.
3. Nếu event đã xử lý thì log và bỏ qua.
4. Nếu chưa xử lý thì gọi EmailService gửi email xác nhận đơn hàng.
5. Chỉ set Redis key sau khi gửi email thành công.
6. Nếu lỗi kết nối Mail Server, cho phép retry tối đa 3 lần.
7. Sau 3 lần thất bại thì chuyển message sang Dead Letter Queue.
8. Code phải có logging bằng `@Slf4j`.

Yêu cầu code:
- Java Spring Boot.
- Dùng `@KafkaListener`.
- Dùng `StringRedisTemplate` hoặc `RedisTemplate<String, String>`.
- Có class `OrderEventListener`.
- Có `EmailService` interface.
- Có DTO `OrderCompletedEvent`.
- Có cấu hình Kafka retry/DLQ nếu cần.
- Code production-friendly, rõ ràng.
```

## 5. Minh chứng thực tế - Log chat bước 1

```markdown
# Bước 1: So sánh Apache Kafka và RabbitMQ cho OrderCompletedEvent

## Apache Kafka

Kafka là distributed event streaming platform, phù hợp với hệ thống có lượng event lớn, cần lưu trữ log sự kiện, replay event và scale consumer theo consumer group.

## RabbitMQ

RabbitMQ là message broker truyền thống, mạnh về routing, exchange, queue, acknowledgement và phù hợp với task queue hoặc workflow cần routing linh hoạt.

## Bảng so sánh

| Tiêu chí | Apache Kafka | RabbitMQ |
|---|---|---|
| Hiệu năng | Rất cao với throughput lớn, ghi tuần tự vào log | Cao nhưng thường thấp hơn Kafka khi throughput cực lớn |
| Scalability | Scale tốt qua partition và consumer group | Scale qua queue/consumer, routing linh hoạt nhưng partition event stream không mạnh bằng Kafka |
| Độ tin cậy | Message được lưu theo retention, có replication | Có durable queue, persistent message, ack/nack |
| Mô hình message | Event log, publish-subscribe theo topic | Queue-based messaging với exchange routing |
| Replay event | Rất mạnh, consumer có thể đọc lại theo offset | Không phải thế mạnh chính, message thường bị remove sau khi ack |
| Độ phức tạp vận hành | Cao hơn, cần quản lý broker, partition, offset, retention | Dễ tiếp cận hơn, phù hợp queue truyền thống |
| Use case phù hợp | Event-driven, audit log, analytics, nhiều consumer độc lập | Task queue, routing phức tạp, command message |

## Trade-off trong SpeedyCart

Với `OrderCompletedEvent`, hệ thống có nhiều consumer độc lập như Email, Loyalty và Warehouse. Trong tương lai có thể thêm Analytics, Recommendation hoặc Fraud Detection. Kafka phù hợp vì event có thể được lưu lại và replay cho consumer mới.

RabbitMQ vẫn phù hợp nếu yêu cầu chính là gửi task đến worker và routing phức tạp, nhưng khả năng replay lịch sử event không mạnh bằng Kafka.

## Khuyến nghị

Nên chọn Apache Kafka cho `OrderCompletedEvent` vì SpeedyCart cần kiến trúc event-driven mở rộng, nhiều consumer độc lập, throughput cao và khả năng replay event. RabbitMQ phù hợp hơn nếu hệ thống đơn giản, chủ yếu cần task queue và vận hành dễ hơn.
```

## 6. Minh chứng thực tế - Log chat bước 2

```markdown
# Bước 2: What-if Email Service sập 3 tiếng và giải pháp chống gửi trùng

## 1. Tin nhắn có bị mất không?

Nếu dùng Kafka, event không bị mất ngay khi Email Service sập vì message được lưu trong topic theo cấu hình retention. Consumer group của Email Service giữ offset. Khi service hoạt động lại, nó tiếp tục đọc từ offset chưa commit.

Nếu dùng RabbitMQ, message cũng có thể không mất nếu queue durable, message persistent và consumer dùng manual acknowledgement. Khi Email Service sập trước khi ack, message vẫn nằm trong queue hoặc được requeue.

## 2. Khi Email Service hoạt động trở lại

Email Service sẽ reconnect broker và tiếp tục consume:

- Kafka: đọc tiếp từ offset cuối cùng đã commit.
- RabbitMQ: nhận lại message chưa ack trong durable queue.

Điều quan trọng là không commit offset hoặc không ack message trước khi xử lý thành công.

## 3. Vấn đề gửi trùng email

Trong hệ phân tán, có thể xảy ra trường hợp:

1. Email đã gửi thành công.
2. Service crash trước khi commit offset.
3. Khi restart, broker gửi lại event.
4. Email bị gửi lần thứ hai.

Vì vậy consumer phải idempotent.

## 4. Redis Idempotency Key Store

Mỗi event có `eventId` duy nhất. Email Service dùng Redis để lưu dấu vết event đã xử lý.

Quy trình:

1. Nhận `OrderCompletedEvent`.
2. Tạo key: `processed:email:{eventId}`.
3. Kiểm tra Redis.
4. Nếu key tồn tại, log `already processed` và bỏ qua.
5. Nếu key chưa tồn tại, gửi email.
6. Sau khi gửi email thành công, set Redis key với TTL, ví dụ 30 ngày.
7. Commit offset hoặc ack message.

## 5. Không nên set key trước khi gửi email

Nếu set Redis key trước khi gửi email, sau đó Mail Server lỗi, event sẽ bị đánh dấu là đã xử lý trong khi email chưa được gửi. Khi retry, consumer thấy key tồn tại và bỏ qua, dẫn đến mất email xác nhận.

Do đó chỉ set key sau khi gửi email thành công.

## 6. Retry và DLQ

Nếu gửi mail thất bại do lỗi tạm thời:

- Retry tối đa 3 lần.
- Nếu vẫn thất bại, chuyển message sang Dead Letter Queue hoặc Dead Letter Topic.
- Log lỗi đầy đủ để đội vận hành xử lý.

## 7. Kết luận

Broker giúp không mất message khi consumer sập, nhưng không tự đảm bảo exactly-once ở tầng side effect như gửi email. Cần kết hợp manual commit/ack, Redis idempotency key và DLQ để đảm bảo mỗi đơn hàng chỉ gửi một email xác nhận.
```

## 7. Minh chứng thực tế - Log chat bước 3

```markdown
# Bước 3: Kafka Consumer tích hợp Redis Idempotency, Retry và DLQ

## 1. Suy luận thiết kế

1. `OrderEventListener` nhận `OrderCompletedEvent` từ topic `order-completed` bằng `@KafkaListener`.
2. Mỗi event có `eventId` duy nhất.
3. Consumer tạo Redis key `processed:email:{eventId}`.
4. Nếu key tồn tại, event đã được xử lý, consumer log và return.
5. Nếu key chưa tồn tại, consumer gọi `EmailService.sendOrderConfirmation(event)`.
6. Chỉ sau khi email gửi thành công mới set Redis key với TTL.
7. Nếu Mail Server lỗi, exception được ném ra để Kafka retry.
8. Sau 3 lần retry thất bại, message được chuyển sang DLT/DLQ.

## 2. OrderCompletedEvent.java

```java
package com.speedycart.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCompletedEvent(
        String eventId,
        String orderId,
        String customerId,
        String customerEmail,
        BigDecimal totalAmount,
        Instant completedAt
) {}
```

## 3. EmailService.java

```java
package com.speedycart.email;

import com.speedycart.event.OrderCompletedEvent;

public interface EmailService {
    void sendOrderConfirmation(OrderCompletedEvent event);
}
```

## 4. OrderEventListener.java

```java
package com.speedycart.email;

import com.speedycart.event.OrderCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private static final String IDEMPOTENCY_KEY_PREFIX = "processed:email:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofDays(30);

    private final StringRedisTemplate redisTemplate;
    private final EmailService emailService;

    @KafkaListener(
            topics = "${app.kafka.topics.order-completed:order-completed}",
            groupId = "${spring.kafka.consumer.group-id:email-service}",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void handleOrderCompleted(OrderCompletedEvent event) {
        validateEvent(event);

        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.eventId();
        log.info("Received OrderCompletedEvent. eventId={}, orderId={}", event.eventId(), event.orderId());

        Boolean alreadyProcessed = redisTemplate.hasKey(idempotencyKey);
        if (Boolean.TRUE.equals(alreadyProcessed)) {
            log.info("Event already processed. Skip sending email. eventId={}, orderId={}", event.eventId(), event.orderId());
            return;
        }

        try {
            emailService.sendOrderConfirmation(event);
            redisTemplate.opsForValue().set(idempotencyKey, "DONE", IDEMPOTENCY_TTL);
            log.info("Order confirmation email sent and idempotency key stored. eventId={}, orderId={}",
                    event.eventId(), event.orderId());
        } catch (RuntimeException ex) {
            log.error("Failed to process OrderCompletedEvent. eventId={}, orderId={}",
                    event.eventId(), event.orderId(), ex);
            throw ex;
        }
    }

    private void validateEvent(OrderCompletedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("OrderCompletedEvent must not be null");
        }
        if (event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (event.orderId() == null || event.orderId().isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (event.customerEmail() == null || event.customerEmail().isBlank()) {
            throw new IllegalArgumentException("customerEmail must not be blank");
        }
    }
}
```

## 5. KafkaConsumerConfig.java

```java
package com.speedycart.config;

import com.speedycart.event.OrderCompletedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, OrderCompletedEvent> orderConsumerFactory() {
        JsonDeserializer<OrderCompletedEvent> deserializer = new JsonDeserializer<>(OrderCompletedEvent.class);
        deserializer.addTrustedPackages("com.speedycart.event");
        deserializer.setUseTypeHeaders(false);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCompletedEvent> orderKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderCompletedEvent> orderConsumerFactory,
            DefaultErrorHandler orderDefaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, OrderCompletedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory);
        factory.setCommonErrorHandler(orderDefaultErrorHandler);
        return factory;
    }

    @Bean
    public DefaultErrorHandler orderDefaultErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Message sent to DLQ. topic={}, partition={}, offset={}, reason={}",
                            record.topic(), record.partition(), record.offset(), ex.getMessage(), ex);
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
        );

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3L);
        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }
}
```

## 6. application.yml ví dụ

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: email-service
      auto-offset-reset: earliest

app:
  kafka:
    topics:
      order-completed: order-completed
```

## 7. Kết luận

Code trên đảm bảo consumer xử lý event theo hướng idempotent: event đã gửi email thành công sẽ có key trong Redis và không gửi lại. Nếu Mail Server lỗi, exception được ném để Kafka retry. Sau 3 lần retry thất bại, message được đưa sang Dead Letter Topic để xử lý thủ công hoặc chạy lại sau.
```
