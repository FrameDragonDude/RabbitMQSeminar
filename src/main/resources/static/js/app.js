'use strict';

const API = {
    orders: '/api/orders',
    bulk: '/api/orders/bulk',
    processed: '/api/orders/processed',
    broadcast: '/api/notifications/broadcast',
    consumerStart: '/api/consumer/start',
    consumerStop: '/api/consumer/stop',
    stats: '/api/monitor/stats',
    events: '/api/monitor/events',
    legacyQueue: '/api/queues/legacy',
    purge: (queue) => `/api/queues/${encodeURIComponent(queue)}/purge`,
};

const DLQ_QUEUE = 'order.dlq';
const ORDER_QUEUE = 'order.queue';
const BULK_PREFIX = 'BULK-';
const STATS_INTERVAL_MS = 1000;
const MAX_LOG_ITEMS = 300;
const GROUP_WINDOW_MS = 5000;

// Giao diện chỉ dùng 3 màu: tím chàm (các queue, đậm nhạt khác nhau), xám và đỏ (DLQ)
const QUEUE_COLORS = {
    'order.queue': '#4f46e5',
    'order.dlq': '#dc2626',
    'notification.email.queue': '#818cf8',
    'notification.sms.queue': '#a5b4fc',
    'notification.audit.queue': '#c7d2fe',
};

const SCENARIO_TITLES = {
    direct: 'Direct exchange',
    idempotency: 'Chống xử lý trùng',
    dlq: 'Retry & DLQ',
    fanout: 'Fanout exchange',
    bulk: 'San tải hàng loạt',
    consumer: 'Dừng / bật consumer',
    tools: 'Dọn dữ liệu demo',
};

/** Cách hiển thị từng loại sự kiện; cat dùng cho bộ lọc nhật ký. */
const EVENT_TYPES = {
    ORDER_SENT:            { label: 'Đã publish đơn',          icon: 'bi-send-fill',            tone: 'primary', cat: 'orders' },
    BULK_SENT:             { label: 'Publish hàng loạt',       icon: 'bi-stack',                tone: 'primary', cat: 'orders' },
    PROCESSING:            { label: 'Đang xử lý',              icon: 'bi-hourglass-split',      tone: 'neutral',  cat: 'orders' },
    SUCCESS:               { label: 'Xử lý xong · ACK',        icon: 'bi-check2-circle',        tone: 'primary',  cat: 'orders' },
    DUPLICATE_SKIPPED:     { label: 'Chặn tin trùng',          icon: 'bi-shield-check',         tone: 'neutral', cat: 'orders' },
    IN_PROGRESS:           { label: 'Đang xử lý ở luồng khác', icon: 'bi-arrow-repeat',         tone: 'neutral',  cat: 'orders' },
    FAILED:                { label: 'Xử lý thất bại',          icon: 'bi-x-circle',             tone: 'danger',   cat: 'errors' },
    DEAD_LETTERED:         { label: 'Chuyển vào DLQ',          icon: 'bi-archive-fill',         tone: 'danger',   cat: 'errors' },
    INVALID_MESSAGE:       { label: 'Tin không đọc được',      icon: 'bi-file-earmark-x',       tone: 'danger',   cat: 'errors' },
    BROADCAST_SENT:        { label: 'Phát thông báo',          icon: 'bi-broadcast',            tone: 'primary',   cat: 'notify' },
    NOTIFICATION_RECEIVED: { label: 'Nhận thông báo',          icon: 'bi-envelope-paper',       tone: 'neutral',   cat: 'notify' },
    CONSUMER_STOPPED:      { label: 'Consumer đã dừng',        icon: 'bi-stop-circle',          tone: 'danger',  cat: 'system' },
    CONSUMER_STARTED:      { label: 'Consumer chạy lại',       icon: 'bi-play-circle',          tone: 'primary',  cat: 'system' },
    QUEUE_PURGED:          { label: 'Dọn dữ liệu',             icon: 'bi-eraser',               tone: 'neutral',  cat: 'system' },
    CLIENT_ERROR:          { label: 'Lỗi gọi API',             icon: 'bi-exclamation-triangle', tone: 'danger',   cat: 'errors' },
};

const CHART = { bucketMs: 2000, buckets: 30 };

const state = {
    lastOrderCode: null,
    consumerRunning: true,
    bulkRows: new Map(),   // mã lô -> { item, count }
    chartOk: new Map(),    // chỉ số bucket -> số đơn thành công
    chartDlq: new Map(),   // chỉ số bucket -> số tin vào DLQ
};

const $ = (id) => document.getElementById(id);

// ================= Tiện ích =================

async function request(method, url, body) {
    const options = { method, headers: {} };
    if (body !== undefined) {
        options.headers['Content-Type'] = 'application/json';
        options.body = JSON.stringify(body);
    }
    const response = await fetch(url, options);
    if (!response.ok) {
        let detail = response.statusText;
        try {
            detail = (await response.json()).message || detail;
        } catch (ignored) {
            // body không phải JSON
        }
        throw new Error(`${response.status} ${detail}`);
    }
    return response.json();
}

/** Khoá nút trong lúc gọi API, báo kết quả bằng toast. */
async function run(button, action, successMessage) {
    if (button) button.disabled = true;
    try {
        const result = await action();
        toast(successMessage || result?.message || 'Thành công');
    } catch (error) {
        toast(error.message, true);
        addLogItem({ timestamp: Date.now(), type: 'CLIENT_ERROR', source: 'browser', message: error.message });
    } finally {
        if (button) button.disabled = false;
        renderResendButton();
    }
}

function toast(message, isError = false) {
    const item = document.createElement('div');
    item.className = `toast-x ${isError ? 'is-error' : 'is-ok'}`;
    const icon = document.createElement('i');
    icon.className = `bi ${isError ? 'bi-x-circle-fill' : 'bi-check-circle-fill'}`;
    const text = document.createElement('span');
    text.textContent = message;
    item.append(icon, text);
    $('toasts').append(item);
    setTimeout(() => item.classList.add('hide'), 2500);
    setTimeout(() => item.remove(), 2900);
}

function newOrderCode(prefix) {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    const random = Math.floor(10 + Math.random() * 90);
    return `${prefix}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}-${random}`;
}

function formatNumber(value) {
    return Number(value).toLocaleString('vi-VN');
}

function formatTime(timestamp) {
    return new Date(timestamp).toLocaleTimeString('vi-VN', { hour12: false });
}

function iconElement(className) {
    const icon = document.createElement('i');
    icon.className = `bi ${className}`;
    return icon;
}

function submitButton(event) {
    return event.submitter || event.target.querySelector('button[type="submit"]');
}

// ================= Điều hướng =================

function selectScenario(name, scroll = true) {
    document.querySelectorAll('[data-pane]').forEach((pane) => {
        pane.hidden = pane.dataset.pane !== name;
    });
    document.querySelectorAll('.nav-item[data-scenario]').forEach((item) => {
        item.classList.toggle('active', item.dataset.scenario === name);
    });
    document.querySelectorAll('.stat-card').forEach((card) => {
        card.classList.toggle('active', card.dataset.scenario === name);
    });
    $('crumbCurrent').textContent = SCENARIO_TITLES[name];
    document.body.classList.remove('sidebar-open');

    if (scroll && window.innerWidth < 1200) {
        document.querySelector(`[data-pane="${name}"]`).scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

function bindNavigation() {
    document.querySelectorAll('[data-scenario]').forEach((element) => {
        element.addEventListener('click', (event) => {
            event.preventDefault();
            selectScenario(element.dataset.scenario);
        });
    });
    $('btnSidebar').addEventListener('click', () => document.body.classList.add('sidebar-open'));
    $('sidebarBackdrop').addEventListener('click', () => document.body.classList.remove('sidebar-open'));
}

// ================= Kịch bản =================

function setLastOrder(code) {
    state.lastOrderCode = code;
    $('lastOrderCode').textContent = code;
    renderResendButton();
}

function renderResendButton() {
    $('btnResend').disabled = state.lastOrderCode === null;
}

function sendOrder(code, simulateError = false) {
    return request('POST', API.orders, { orderCode: code, simulateError });
}

function sendBulk(count) {
    return request('POST', `${API.bulk}?count=${count}`);
}

const ACTIONS = {
    'resend': (button) => run(button, () => sendOrder(state.lastOrderCode)),

    'send-twice': (button) => {
        const code = newOrderCode('ORD');
        run(button, async () => {
            await sendOrder(code);
            await sendOrder(code);
            setLastOrder(code);
        }, `Đã gửi ${code} hai lần`);
    },

    'error-order': (button) => run(button, () => sendOrder(newOrderCode('ERR'), true)),

    'toggle-consumer': (button) => {
        const url = state.consumerRunning ? API.consumerStop : API.consumerStart;
        run(button, async () => {
            const result = await request('POST', url);
            await refreshStats();
            return result;
        });
    },

    'bulk-20': (button) => run(button, () => sendBulk(20)),

    'reset-processed': (button) => run(button, () => request('DELETE', API.processed)),

    'delete-legacy': (button) => {
        if (confirm('Xoá queue cũ notification.queue cùng toàn bộ tin trong đó?')) {
            run(button, () => request('DELETE', API.legacyQueue));
        }
    },
};

function bindScenarios() {
    $('orderCode').value = newOrderCode('ORD');

    $('formOrder').addEventListener('submit', (event) => {
        event.preventDefault();
        const code = $('orderCode').value.trim();
        run(submitButton(event), async () => {
            const result = await sendOrder(code);
            setLastOrder(code);
            $('orderCode').value = newOrderCode('ORD');
            return result;
        });
    });

    $('formBroadcast').addEventListener('submit', (event) => {
        event.preventDefault();
        const message = $('broadcastMessage').value.trim();
        run(submitButton(event), () => request('POST', API.broadcast, { message }));
    });

    $('formBulk').addEventListener('submit', (event) => {
        event.preventDefault();
        run(submitButton(event), () => sendBulk(Number($('bulkCount').value)));
    });

    document.querySelectorAll('[data-bulk]').forEach((button) => {
        button.addEventListener('click', () => {
            $('bulkCount').value = button.dataset.bulk;
            run(button, () => sendBulk(Number(button.dataset.bulk)));
        });
    });

    document.querySelectorAll('[data-action]').forEach((button) => {
        button.addEventListener('click', () => ACTIONS[button.dataset.action](button));
    });

    document.querySelectorAll('[data-purge]').forEach((button) => {
        button.addEventListener('click', () => {
            const queue = button.dataset.purge;
            if (confirm(`Xoá toàn bộ tin đang chờ trong ${queue}?`)) {
                run(button, () => request('POST', API.purge(queue)));
            }
        });
    });
}

// ================= Số liệu từ broker =================

async function refreshStats() {
    try {
        renderStats(await request('GET', API.stats));
    } catch (ignored) {
        // mất kết nối: giữ số liệu cũ, trạng thái kết nối do EventSource hiển thị
    }
}

function renderStats(stats) {
    state.consumerRunning = stats.consumerRunning;
    const byName = Object.fromEntries(stats.queues.map((q) => [q.queue, q]));

    const consumer = $('statConsumer');
    consumer.textContent = stats.consumerRunning ? 'Đang chạy' : 'Đã dừng';
    consumer.className = `stat-value stat-value-text ${stats.consumerRunning ? 'is-ok' : 'is-bad'}`;
    const consumerIcon = $('statConsumerIcon');
    consumerIcon.className = `stat-icon ${stats.consumerRunning ? 'icon-primary' : 'icon-danger'}`;
    consumerIcon.replaceChildren(iconElement(stats.consumerRunning ? 'bi-cpu' : 'bi-pause-circle'));

    $('statProcessed').textContent = formatNumber(stats.processedOrders);
    $('statDuplicates').textContent = formatNumber(stats.duplicateSkipped);
    $('statDlq').textContent = formatNumber(byName[DLQ_QUEUE]?.ready ?? 0);

    $('toolOrderReady').textContent = formatNumber(byName[ORDER_QUEUE]?.ready ?? 0);
    $('toolDlqReady').textContent = formatNumber(byName[DLQ_QUEUE]?.ready ?? 0);

    const legacy = stats.legacyQueueReady;
    $('legacyAlert').hidden = legacy === null || legacy === undefined;
    $('legacyCount').textContent = formatNumber(legacy ?? 0);
    $('toolLegacy').textContent = legacy == null ? 'Đã xoá' : `Queue của bản cũ · ${formatNumber(legacy)} tin`;

    renderConsumerButton();
    renderQueues(stats.queues);
}

function renderConsumerButton() {
    const button = $('btnConsumer');
    if (state.consumerRunning) {
        button.className = 'btn-x btn-dark-x';
        button.replaceChildren(iconElement('bi-stop-circle-fill'), ' Dừng consumer');
    } else {
        button.className = 'btn-x btn-primary-x';
        button.replaceChildren(iconElement('bi-play-circle-fill'), ' Khởi động consumer');
    }
}

function renderQueues(queues) {
    const total = queues.reduce((sum, q) => sum + q.ready, 0);
    $('queueTotal').textContent = formatNumber(total);

    const segments = queues.filter((q) => q.ready > 0).map((q) => {
        const segment = document.createElement('span');
        segment.style.width = `${(q.ready / total) * 100}%`;
        segment.style.background = QUEUE_COLORS[q.queue];
        segment.title = `${q.queue}: ${q.ready}`;
        return segment;
    });
    $('queueBar').replaceChildren(...segments);

    const rows = queues.map((q) => {
        const row = document.createElement('li');

        const dot = document.createElement('span');
        dot.className = 'queue-dot';
        dot.style.background = QUEUE_COLORS[q.queue];

        const info = document.createElement('div');
        const name = document.createElement('div');
        name.className = 'queue-name';
        name.textContent = q.queue;
        const route = document.createElement('div');
        route.className = 'queue-route';
        route.textContent = `${q.exchangeType} · ${q.routingKey || 'không routing key'}`;
        info.append(name, route);

        const ready = document.createElement('div');
        ready.className = 'queue-ready'
            + (q.ready === 0 ? ' is-zero' : '')
            + (q.queue === DLQ_QUEUE && q.ready > 0 ? ' is-bad' : '');
        ready.textContent = formatNumber(q.ready);
        const consumers = document.createElement('span');
        consumers.className = 'queue-consumers' + (q.consumers === 0 && q.queue !== DLQ_QUEUE ? ' is-none' : '');
        consumers.textContent = `${q.consumers} consumer`;
        ready.append(consumers);

        row.append(dot, info, ready);
        return row;
    });
    $('queueList').replaceChildren(...rows);
}

// ================= Biểu đồ thông lượng =================

function countForChart(type) {
    const bucket = Math.floor(Date.now() / CHART.bucketMs);
    const target = type === 'SUCCESS' ? state.chartOk
        : (type === 'DEAD_LETTERED' || type === 'INVALID_MESSAGE') ? state.chartDlq
        : null;
    if (target) {
        target.set(bucket, (target.get(bucket) || 0) + 1);
    }
}

function renderChart() {
    const current = Math.floor(Date.now() / CHART.bucketMs);
    const first = current - CHART.buckets + 1;
    const data = [];
    for (let b = first; b <= current; b++) {
        data.push({ ok: state.chartOk.get(b) || 0, dlq: state.chartDlq.get(b) || 0 });
    }
    for (const map of [state.chartOk, state.chartDlq]) {
        for (const key of map.keys()) {
            if (key < first) map.delete(key);
        }
    }

    const peak = Math.max(...data.map((d) => d.ok + d.dlq));
    const max = Math.max(4, Math.ceil(peak / 4) * 4);
    $('chartMax').textContent = max;
    $('chartMid').textContent = max / 2;
    $('chartOkTotal').textContent = formatNumber(data.reduce((s, d) => s + d.ok, 0));
    $('chartDlqTotal').textContent = formatNumber(data.reduce((s, d) => s + d.dlq, 0));

    const bars = data.map((d) => {
        const bar = document.createElement('div');
        bar.className = 'bar';
        if (d.ok + d.dlq === 0) {
            const empty = document.createElement('div');
            empty.className = 'bar-seg bar-empty';
            bar.append(empty);
        } else {
            for (const [value, cls] of [[d.ok, 'bar-ok'], [d.dlq, 'bar-dlq']]) {
                if (value > 0) {
                    const segment = document.createElement('div');
                    segment.className = `bar-seg ${cls}`;
                    segment.style.height = `${(value / max) * 100}%`;
                    bar.append(segment);
                }
            }
        }
        bar.title = `${d.ok} thành công · ${d.dlq} vào DLQ`;
        return bar;
    });
    $('chartBars').replaceChildren(...bars);
}

// ================= Nhật ký sự kiện (SSE) =================

function connectEvents() {
    const source = new EventSource(API.events);
    source.addEventListener('open', () => setConnection(true));
    source.addEventListener('error', () => setConnection(false)); // EventSource tự kết nối lại
    source.addEventListener('demo', (message) => handleEvent(JSON.parse(message.data)));
}

function setConnection(live) {
    $('connection').className = `conn-pill ${live ? 'is-live' : 'is-down'}`;
    $('connectionText').textContent = live ? 'Đã kết nối' : 'Mất kết nối…';
}

function handleEvent(event) {
    countForChart(event.type);
    if (event.type === 'SUCCESS' && event.orderCode?.startsWith(BULK_PREFIX)) {
        updateBulkRow(event);
        return;
    }
    addLogItem(event);
}

/** Đơn bulk xử lý rất nhanh: gộp thành một dòng đếm theo lô. */
function updateBulkRow(event) {
    const batch = event.orderCode.split('-').slice(0, 2).join('-'); // BULK-3-0042 -> BULK-3
    let entry = state.bulkRows.get(batch);
    if (!entry || !entry.item.isConnected) {
        entry = { item: addLogItem({ ...event, orderCode: batch, message: '' }), count: 0 };
        state.bulkRows.set(batch, entry);
    }
    entry.count += 1;
    entry.item.querySelector('.log-time').textContent = formatTime(event.timestamp);
    entry.item.querySelector('.log-text').textContent = `Đã xử lý ${formatNumber(entry.count)} đơn của lô`;
}

/** Thêm một dòng nhật ký; sự kiện giống hệt dòng trên cùng (trong vài giây) được gộp thành "×N". */
function addLogItem(event) {
    const log = $('eventLog');
    log.querySelector('.log-empty')?.remove();

    const groupKey = [event.type, event.source, event.orderCode, event.message].join('|');
    const top = log.firstElementChild;
    if (top && top.dataset.key === groupKey && event.timestamp - Number(top.dataset.time) < GROUP_WINDOW_MS) {
        const count = Number(top.dataset.count) + 1;
        top.dataset.count = count;
        top.dataset.time = event.timestamp;
        top.querySelector('.log-time').textContent = formatTime(event.timestamp);
        let badge = top.querySelector('.log-count');
        if (!badge) {
            badge = document.createElement('span');
            badge.className = 'log-count';
            top.querySelector('.log-title').append(badge);
        }
        badge.textContent = `×${formatNumber(count)}`;
        return top;
    }

    const style = EVENT_TYPES[event.type] || EVENT_TYPES.PROCESSING;
    const item = document.createElement('li');
    item.className = 'log-item';
    item.dataset.cat = style.cat;
    item.dataset.key = groupKey;
    item.dataset.time = event.timestamp;
    item.dataset.count = 1;

    const icon = document.createElement('span');
    icon.className = `log-icon tone-${style.tone}`;
    icon.append(iconElement(style.icon));

    const body = document.createElement('div');
    body.className = 'log-body';
    const title = document.createElement('div');
    title.className = 'log-title';
    title.append(style.label);
    if (event.orderCode) {
        const code = document.createElement('span');
        code.className = 'log-code';
        code.textContent = event.orderCode;
        title.append(code);
    }
    const text = document.createElement('div');
    text.className = 'log-text';
    text.textContent = event.message || '';
    body.append(title, text);

    const meta = document.createElement('div');
    meta.className = 'log-meta';
    const time = document.createElement('div');
    time.className = 'log-time';
    time.textContent = formatTime(event.timestamp);
    const source = document.createElement('div');
    source.className = 'log-source';
    source.textContent = event.source;
    meta.append(time, source);

    item.append(icon, body, meta);
    log.prepend(item);

    while (log.children.length > MAX_LOG_ITEMS) {
        log.lastElementChild.remove();
    }
    return item;
}

function bindLog() {
    $('btnClearLog').addEventListener('click', () => {
        $('eventLog').replaceChildren();
        state.bulkRows.clear();
    });

    document.querySelectorAll('[data-filter]').forEach((chip) => {
        if (chip.tagName !== 'BUTTON') return;
        chip.addEventListener('click', () => {
            document.querySelectorAll('.filter-row .chip-btn').forEach((c) => c.classList.toggle('active', c === chip));
            $('eventLog').dataset.filter = chip.dataset.filter;
        });
    });
}

// ================= Khởi động =================

bindNavigation();
bindScenarios();
bindLog();
selectScenario('direct', false);
connectEvents();
refreshStats();
renderChart();
setInterval(refreshStats, STATS_INTERVAL_MS);
setInterval(renderChart, 1000);
