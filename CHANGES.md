# Thay đổi cho demo RabbitMQ Seminar

Ngày cập nhật: 31/10/2026 (3 đợt)

## Đợt 3: giao diện 3 màu + demo bằng Postman

### Giao diện chỉ dùng 3 màu
| Màu | Dùng cho |
|---|---|
| Tím chàm `#4f46e5` | Thao tác chính, trạng thái bình thường (consumer đang chạy, xử lý xong), mục đang chọn |
| Xám (slate) | Khung, chữ, nền, sự kiện trung tính |
| Đỏ `#dc2626` | Lỗi, DLQ, consumer đã dừng, thao tác xoá |

Bỏ toàn bộ gradient. Icon dùng nền nhạt cùng màu, nút dùng màu đặc. Các queue thông báo phân biệt bằng độ đậm nhạt của tím chàm.

### API theo dõi hành trình: `GET /api/tracking/{mã}`
Mỗi sự kiện đều được lưu theo mã đơn / mã thông báo (`service/TrackingService`, giữ 2.000 mã gần nhất). Kết quả gồm:
- `status`: `QUEUED`, `PROCESSING`, `RETRYING`, `DONE`, `DEAD_LETTERED` hoặc `BROADCAST`;
- `summary`: câu tóm tắt, vd. "Đã xử lý thành công 1 lần, 1 tin trùng bị bỏ qua";
- `attempts`, `processedTimes`, `duplicatesSkipped`, `receivedBy`;
- `events`: timeline đầy đủ với thời gian tới mili giây.

Các API gửi trả thêm `trackingUrl`. Thông báo fanout giờ có mã `NTF-<n>` (payload `NotificationMessage`), nên tra được service nào đã nhận.

### Postman collection: `postman/RabbitMQ-Seminar.postman_collection.json`
Import vào Postman (**Import → chọn file**). Collection có 8 thư mục, khớp với các kịch bản:

| Thư mục | Nội dung | Kiểm tra tự động |
|---|---|---|
| 0. Chuẩn bị | Xem trạng thái, bật consumer, purge DLQ, xoá queue cũ, xoá bộ nhớ chống trùng | — |
| 1. Direct exchange | Gửi đơn → xem hành trình → gọi Management API xem `order.queue` | `DONE`, xử lý đúng 1 lần, queue có DLX |
| 2. Chống xử lý trùng | Gửi lại đúng mã đơn → xem hành trình | `processedTimes = 1`, có tin trùng bị bỏ qua |
| 3. Retry & DLQ | Gửi đơn lỗi → xem lúc đang retry → xem lúc đã vào DLQ → đọc tin trong `order.dlq` qua Management API | `RETRYING` → `DEAD_LETTERED`, 3 lần xử lý, header `x-death` reason `rejected` |
| 4. Fanout exchange | Phát thông báo → xem service nào nhận → xem binding của exchange | Đủ 3 service, bind tới 3 queue |
| 5. San tải | Gửi 300 đơn → xem queue (bấm Send nhiều lần) | — |
| 6. Consumer sập | Dừng → gửi 20 đơn → kiểm tra → bật lại → kiểm tra | 0 consumer và Ready ≥ 20 → có consumer và Ready giảm |
| 7. SSE | Luồng sự kiện thời gian thực | — |

Điểm làm cho demo trực quan:
- **Tự chạy**: mã đơn tự sinh, các bước "chờ N giây" đã có sẵn trong Pre-request Script, nên chạy được cả thư mục bằng **Runner**.
- **Tab Test Results**: mỗi kịch bản có assertion pass/fail rõ ràng, có thể nói "hệ thống tự chứng minh".
- **Tab Visualize**: kết quả `/api/tracking`, `/api/monitor/stats` và tin trong DLQ hiện thành bảng (timeline, thanh Ready, header `x-death`).
- **Đối chiếu với broker**: các request "RabbitMQ API" gọi thẳng Management HTTP API (cổng 15672, Basic Auth `seminar`/`seminar123`) để chứng minh dữ liệu không phải do app tự báo.
- Biến dùng chung: `baseUrl`, `rabbitUrl`, `vhost`, `rabbitUser`, `rabbitPass` (tab **Variables** của collection).

Đã chạy thử toàn bộ collection bằng Newman trên vhost test: **24 request, 26/26 assertion pass**.

### Gợi ý trình bày với Postman
Chia màn hình: trái là Postman, phải là trang dashboard (hoặc Management UI).
1. Chạy *0. Chuẩn bị*.
2. Mỗi kịch bản: bấm Send từng request theo thứ tự. Sau mỗi bước chỉ vào tab **Visualize** / **Test Results**, đồng thời chỉ sang dashboard thấy biểu đồ và nhật ký thay đổi.
3. Kết thúc: mở **Runner**, chạy lại cả collection để cho thấy toàn bộ kịch bản pass trong khoảng 25 giây.

---

## Đợt 2: sửa lỗi khi chạy với dữ liệu cũ + giao diện dashboard

### Nguyên nhân "lỗi tùm lum"
Khi chạy bản mới, `order.queue` vẫn còn khoảng 1.500 tin của bản cũ (dạng Java serialization). Bản mới đọc JSON nên
không đọc được các tin này, nhưng vẫn **retry mỗi tin 3 lần** (khoảng 2 giây một tin). Hậu quả: queue bị tắc, đơn mới
phải xếp sau hơn 1.500 tin rác, console đầy stack trace, nhật ký toàn dòng "Chuyển vào DLQ".

### Đã sửa
- **Không retry lỗi vô ích** (`RabbitMQConfig.listenerRetryCustomizer`): tin không chuyển được sang `OrderMessage`
  (`MessageConversionException`) hoặc tin bị chủ động reject sẽ đi thẳng vào DLQ, không retry. Lỗi nghiệp vụ vẫn retry
  đủ 3 lần như cũ.
  - Đã kiểm tra: 30 tin dạng cũ vào DLQ trong dưới 1 giây (trước đây khoảng 60 giây); đơn mới vẫn xử lý bình thường.
- Sự kiện mới `INVALID_MESSAGE` ("Tin không đọc được"), kèm content-type để biết vì sao.
- **Log gọn**: tắt các stack trace lặp lại của Spring AMQP. Mỗi lỗi chỉ còn một dòng do `DemoEventPublisher` ghi.
- **Dọn dữ liệu ngay trên giao diện** (menu *Công cụ → Dọn dữ liệu demo*):
  - `POST /api/queues/{name}/purge`: xoá tin trong `order.queue`, `order.dlq` hoặc các queue thông báo (tên khác bị từ chối).
  - `DELETE /api/queues/legacy`: xoá queue cũ `notification.queue`.
  - Giao diện tự hiện cảnh báo khi phát hiện queue cũ này.
- Nhật ký tự gộp các sự kiện giống nhau liên tiếp thành một dòng `×N`, không còn bị tràn.

### Giao diện mới (kiểu admin dashboard)
- **Sidebar** có nhóm *Kịch bản demo* và *Công cụ*; mục đang chọn là pill gradient. Thu gọn thành menu trượt trên điện thoại.
- **4 thẻ thống kê** có icon gradient: Consumer, Đã xử lý, Tin trùng, Trong DLQ. Bấm vào thẻ sẽ mở kịch bản liên quan.
- **Biểu đồ thông lượng** thời gian thực: 60 giây gần nhất, mỗi cột 2 giây, chia hai màu "thành công" và "vào DLQ".
- **Thẻ Hàng đợi**: thanh tỷ lệ tin Ready giữa các queue, kèm số consumer từng queue.
- **Khung kịch bản**: mỗi lần hiện một kịch bản, có mô tả, mục "Quan sát" (nhìn vào đâu khi trình bày) và các nút thao tác.
  Thêm nút "Gửi một đơn mới 2 lần liên tiếp" để demo chống trùng chỉ với một cú bấm.
- **Nhật ký sự kiện**: icon theo loại sự kiện, bộ lọc (Tất cả / Đơn hàng / Lỗi & DLQ / Thông báo / Hệ thống).
- Có thông báo nhỏ (toast) cho kết quả mỗi thao tác; hỏi xác nhận trước khi purge hoặc xoá queue.
- Font Nunito lưu cục bộ trong `static/vendor/nunito` (chỉ gồm các subset latin và tiếng Việt), chạy được khi không có mạng.

---

## Tóm tắt (đợt 1)

| # | Vấn đề trước đây | Đã sửa |
|---|---|---|
| 1 | "Tạm dừng consumer" dùng vòng `while + sleep`, prefetch mặc định 250 nên tin dồn ở **Unacked**, cột **Ready** không tăng | Dừng/bật thật listener container, `prefetch=1` |
| 2 | Listener của `order.dlq` lấy tin ngay, DLQ trên Dashboard luôn bằng 0 | Bỏ listener DLQ, tin lỗi nằm lại để kiểm tra |
| 3 | Fanout chỉ bind 1 queue, không thấy được broadcast | 3 queue: email, sms, audit, mỗi queue một consumer |
| 4 | Message dạng Java serialization, "Get messages" không đọc được | Dùng JSON (`Jackson2JsonMessageConverter`) |
| 5 | Giao diện báo "thành công", "vào DLQ" bằng `setTimeout`, không phải trạng thái thật | Số liệu đọc từ broker mỗi giây + sự kiện thật qua SSE |
| 6 | Chưa có chống trùng | Idempotent consumer theo `orderCode` |
| 7 | Giao diện nhiều màu, emoji, có ghi chú cho người thuyết trình hiển thị trên máy chiếu | Giao diện trung tính, chỉ dùng Bootstrap Icons, bỏ ghi chú riêng |
| 8 | Sơ đồ sai tên exchange, routing key, HTTP method | Vẽ lại khớp với code |
| 9 | `application.properties` có BOM, thiếu plugin đóng gói, CSS/icon tải từ CDN | Đã sửa, thêm `docker-compose.yml`, CSS/icon lưu cục bộ |

---

## 1. Sửa lỗi demo (backend)

### 1.1 Dừng consumer thật sự
- File mới: `service/ConsumerControlService.java`. Dùng `RabbitListenerEndpointRegistry` để `stop()`/`start()` listener có id `orderListener`.
- Khi dừng, consumer huỷ subscribe khỏi broker. Tin mới nằm ở **Ready**, cột **Consumers** của `order.queue` về **0**, đúng như khi service chết thật.
- `spring.rabbitmq.listener.simple.prefetch=1`: mỗi lần consumer chỉ giữ 1 tin chưa ACK, nên khi test tải sẽ thấy cột Ready giảm dần.
- Đã kiểm tra: dừng consumer rồi gửi 20 đơn → Ready = 20, Consumers = 0. Bật lại → queue rút về 0, không mất tin.

### 1.2 Dead Letter Queue
- Bỏ `@RabbitListener` trên `order.dlq`. Tin lỗi nằm lại trong queue, mở Management UI → `order.dlq` → **Get messages** sẽ thấy header `x-death` (reason `rejected`, queue gốc `order.queue`) và payload JSON.
- `config/DeadLetterRecoverer.java`: được gọi khi hết retry. Giữ nguyên hành vi mặc định (reject, không requeue, broker chuyển sang DLX) và phát thêm sự kiện lên giao diện.
- Retry khai báo rõ ràng: tổng cộng **3 lần xử lý** (1 lần đầu và 2 lần retry), cách nhau 1 giây. Đơn lỗi mất khoảng 8 giây mới vào DLQ (3 × 2 giây xử lý + 2 × 1 giây chờ).

### 1.3 Fanout exchange
- `system.fanout.exchange` bind tới 3 queue: `notification.email.queue`, `notification.sms.queue`, `notification.audit.queue`.
- `consumer/NotificationConsumer.java`: 3 listener mô phỏng 3 service độc lập. Gửi 1 thông báo thì nhật ký hiện 3 dòng nhận.

### 1.4 Message dạng JSON
- Bean `jsonMessageConverter` trong `RabbitMQConfig`. Spring Boot tự áp cho cả `RabbitTemplate` và listener.
- `OrderMessage` không cần `Serializable` nữa.

### 1.5 Số liệu và sự kiện thật cho giao diện
- `GET /api/monitor/stats`: trạng thái consumer, số đơn đã xử lý, số tin trùng đã chặn, cùng số Ready/Consumers của 5 queue (đọc trực tiếp từ broker qua `AmqpAdmin`).
- `GET /api/monitor/events`: Server-Sent Events. Producer, consumer và recoverer phát sự kiện thật (gửi, đang xử lý, thành công, thất bại lần n/3, vào DLQ, trùng...).
- Log console chuyển từ `System.out` sang SLF4J.

---

## 2. Chống trùng orderId (Idempotent Consumer)

### Vì sao phải chống trùng ở consumer
RabbitMQ chỉ đảm bảo giao tin **ít nhất một lần** (at-least-once). Cùng một đơn có thể đến consumer nhiều lần khi:
- producer gửi lại (timeout, người dùng bấm 2 lần);
- consumer xử lý xong nhưng chết trước khi ACK, broker giao lại cho consumer khác.

Vì vậy consumer phải tự đảm bảo mỗi `orderCode` chỉ được xử lý thành công **đúng một lần**.

### Cách làm
- Producer đặt `messageId = orderCode` (`OrderProducer`), nên khoá chống trùng đi kèm message và hiện được trên Management UI và trong DLQ.
- `service/ProcessedOrderStore.java` lưu trạng thái từng mã đơn. Thao tác giữ chỗ dùng `putIfAbsent` nên là nguyên tử, an toàn khi có nhiều consumer chạy song song.

| Kết quả `tryStart(orderCode)` | Ý nghĩa | Consumer làm gì |
|---|---|---|
| `ACQUIRED` | Chưa ai xử lý | Xử lý. Thành công → đánh dấu `DONE`. Lỗi → nhả chỗ để retry |
| `DONE` | Đã xử lý thành công trước đó | **Tin trùng**: bỏ qua, vẫn ACK để xoá khỏi queue |
| `IN_PROGRESS` | Luồng khác đang xử lý dở | Chưa chắc là trùng (luồng kia có thể thất bại): ném `OrderInProgressException` để retry; hết lượt thì **requeue**, không đưa vào DLQ |

### Ca biên đã kiểm tra
Dừng consumer khi đơn lỗi đang retry: broker giao lại tin trong lúc luồng cũ vẫn giữ mã đơn. Bản đầu tiên coi đây là tin trùng và ACK luôn, nên **tin bị mất**. Đã sửa bằng trạng thái `IN_PROGRESS` ở trên. Sau khi sửa, tin vào DLQ đúng một lần và không mất.

### Giới hạn của bản demo
- Dữ liệu lưu trong bộ nhớ, khởi động lại app là mất. Thực tế nên dùng bảng DB có **unique constraint** trên `orderCode` (cùng transaction với nghiệp vụ), hoặc Redis `SET key NX EX ...`.
- Mã đơn bulk có dạng `BULK-<lô>-<số thứ tự>`, mỗi lần gửi là một lô mới nên không bị chặn nhầm.
- `DELETE /api/orders/processed` xoá bộ nhớ chống trùng khi muốn chạy lại demo từ đầu.

### Cách demo
Kịch bản 1: gửi đơn → kịch bản 2: bấm **Gửi lại đơn ...** → nhật ký hiện "Mã đơn đã được xử lý trước đó → bỏ qua", ô **Tin trùng đã chặn** tăng 1, ô **Đơn đã xử lý** giữ nguyên.

---

## 3. Giao diện

- Tách thành `index.html`, `css/app.css`, `js/app.js`.
- Nền sáng, tông màu trung tính, chỉ một màu nhấn (cam RabbitMQ) ở logo. Màu chỉ dùng cho trạng thái (xanh: đang chạy, vàng: đã dừng hoặc tin trùng, đỏ: DLQ).
- Bỏ toàn bộ emoji, thay bằng **Bootstrap Icons**.
- Bỏ các ghi chú dành riêng cho người thuyết trình đang hiện trên màn hình.
- Bố cục:
  - 4 ô chỉ số: trạng thái consumer, đơn đã xử lý, tin trùng đã chặn, tin trong DLQ.
  - 6 kịch bản: Direct, Chống trùng, Retry + DLQ, Fanout, San tải, Consumer sập/phục hồi.
  - Bảng **Exchange → Queue** với số Ready/Consumers thật, cập nhật mỗi giây.
  - **Nhật ký sự kiện** nhận qua SSE. Đơn bulk được gộp thành 1 dòng đếm theo lô.
  - Chỉ báo kết nối "Đã kết nối" / "Mất kết nối".
- Nội dung động được gán bằng `textContent`, không dùng `innerHTML` (tránh chèn HTML từ ô nhập).
- Bootstrap 5.3.3 và Bootstrap Icons 1.11.3 lưu trong `static/vendor`, chạy được khi phòng seminar không có mạng.

---

## 4. Sơ đồ kiến trúc

`architecture-diagram.svg` (ở thư mục gốc và trong `static/`, hai bản giống nhau) được vẽ lại:
- tên exchange, queue, routing key, API khớp với code;
- thêm 3 queue fanout và `NotificationConsumer`;
- đường dead-letter đi từ `order.queue` (reject) → `order.dlx.exchange` → `order.dlq`.

Mở từ nút **Sơ đồ** trên giao diện.

---

## 5. Cấu hình và môi trường

- `application.properties`: bỏ BOM, thêm `prefetch=1`, cấu hình retry đầy đủ, `server.error.include-message=always`, thêm `demo.processing.order-ms` / `bulk-ms` để chỉnh thời gian xử lý giả lập.
- `pom.xml`: thêm `spring-boot-maven-plugin`. Giờ chạy được `mvn spring-boot:run` hoặc `java -jar target/RabbitMQSeminar-0.0.1-SNAPSHOT.jar`.
- `docker-compose.yml`: RabbitMQ 3 management, user `seminar` / `seminar123`.

---

## 6. API

| Cũ | Mới |
|---|---|
| `GET /api/send-order?code=&error=` | `POST /api/orders` body `{"orderCode":"ORD-1","simulateError":false}` |
| `GET /api/bulk-orders?count=` | `POST /api/orders/bulk?count=100` (1 đến 5000) |
| `GET /api/broadcast?msg=` | `POST /api/notifications/broadcast` body `{"message":"..."}` |
| `POST /api/pause-consumer` | `POST /api/consumer/stop` |
| `POST /api/resume-consumer` | `POST /api/consumer/start` |
| `GET /api/consumer-status` | Nằm trong `GET /api/monitor/stats` |
| mới | `GET /api/monitor/events` (SSE) |
| mới | `DELETE /api/orders/processed` |
| mới (đợt 2) | `POST /api/queues/{name}/purge` |
| mới (đợt 2) | `DELETE /api/queues/legacy` |
| mới (đợt 3) | `GET /api/tracking/{reference}` |

Các API ghi dữ liệu chuyển sang `POST`/`DELETE`. Dữ liệu sai trả `400` kèm thông báo (vd. `orderCode là bắt buộc`). Xem đầy đủ ở Swagger UI.

---

## 7. Cấu trúc mã nguồn

```
com.seminar.rabbitmq
├── config      RabbitMQConfig (topology, JSON converter), DeadLetterRecoverer
├── controller  OrderController, NotificationController, ConsumerController, MonitorController,
│               QueueController, TrackingController
├── producer    OrderProducer
├── consumer    OrderConsumer, NotificationConsumer, OrderInProgressException
├── service     ProcessedOrderStore (chống trùng), ConsumerControlService, QueueStatsService, TrackingService
├── event       DemoEvent, DemoEventType, DemoEventPublisher (SSE)
└── dto         OrderMessage, NotificationMessage, OrderRequest, BroadcastRequest, SendResult,
                QueueStat, DemoStats, TrackingResult

postman/RabbitMQ-Seminar.postman_collection.json   Postman collection cho buổi demo
```

`DemoController` cũ đã được xoá, chức năng chia sang 4 controller trên.

---

## 8. Việc cần làm trước khi chạy bản mới ⚠️

1. **Tắt app** đang chạy ở cổng 8080, reload Maven trong IDE (vì `pom.xml` đã đổi), rồi chạy lại.
2. Mở http://localhost:8080. Các tin dạng cũ còn lại trong `order.queue` sẽ tự chuyển sang DLQ trong vài giây.
3. Vào *Công cụ → Dọn dữ liệu demo*:
   - **Purge** `order.dlq` để DLQ về 0 trước khi trình bày;
   - **Xoá** `notification.queue` (queue cũ, giao diện cũng hiện cảnh báo ở đầu trang);
   - **Xoá** bộ nhớ chống trùng nếu muốn đếm lại từ đầu.

---

## 9. Gợi ý trình tự thuyết trình

1. Mở **Sơ đồ** để giới thiệu topology.
2. **Direct**: gửi 1 đơn → nhật ký hiện publish → đang xử lý → ACK.
3. **Chống trùng**: gửi lại đúng mã đó → bị bỏ qua, ô "Tin trùng đã chặn" tăng.
4. **Fanout**: phát 1 thông báo → email, sms, audit đều nhận.
5. **Consumer sập**: dừng consumer → gửi 100 đơn → bảng hiện Ready = 100, Consumers = 0 (đối chiếu Management UI) → bật lại → Ready giảm dần về 0.
6. **Retry + DLQ**: gửi đơn lỗi → thất bại lần 1/3, 2/3, 3/3 → vào DLQ → mở `order.dlq` trên Management UI, **Get messages** để xem header `x-death`.

---

## 10. Chưa xử lý (cần bạn quyết định)

- File `query` ở thư mục gốc (chỉ chứa chữ "RabbitMQ"), có vẻ là file thừa.
- `Clonerepo/rabbitmq-tutorials` được commit dưới dạng submodule nhưng thiếu `.gitmodules`, nên người khác clone về sẽ được thư mục rỗng.
- Dependency `lombok` có trong `pom.xml` nhưng code không dùng.

---

## Đã kiểm thử

Chạy app bản mới trên một vhost riêng (`seminar-test`, đã xoá sau khi test), không đụng vào dữ liệu của vhost `/`:

| Kịch bản | Kết quả |
|---|---|
| Gửi đơn thường | publish → xử lý → ACK, đếm "đã xử lý" +1 |
| Gửi lại cùng mã | `DUPLICATE_SKIPPED`, không xử lý lại |
| Đơn lỗi | thất bại lần 1/3, 2/3, 3/3 → `order.dlq`, header `x-death` đúng, payload JSON |
| Fanout | 1 thông báo → 3 service nhận |
| Dừng consumer + gửi 20 đơn | Ready = 20, Consumers = 0; bật lại → rút về 0, đủ 20 đơn |
| Dừng consumer giữa lúc retry | Tin vào DLQ đúng 1 lần, không bị coi là trùng |
| Dữ liệu sai (`orderCode` rỗng, `count=0`) | 400 kèm thông báo |
| Giao diện | Kết nối SSE, bảng queue và nhật ký cập nhật đúng (đã chụp bằng Chrome headless) |
