-- common-ai `data_investigation` DAG를 위한 샘플 데이터.
-- @task.agent의 SQLToolset이 찾아낼 거리가 되도록, 의도적인 이상치 몇 개를
-- 심어 두었다.

CREATE TABLE customers (
    customer_id  INT PRIMARY KEY,
    name         TEXT        NOT NULL,
    region       TEXT        NOT NULL,
    signup_date  DATE        NOT NULL
);

CREATE TABLE orders (
    order_id     INT PRIMARY KEY,
    customer_id  INT         NOT NULL REFERENCES customers(customer_id),
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT        NOT NULL,
    order_ts     TIMESTAMP   NOT NULL
);

INSERT INTO customers (customer_id, name, region, signup_date) VALUES
    (1, '아크메 상사',   'us-east', '2025-01-04'),
    (2, '글로벡스',      'eu-west', '2025-02-11'),
    (3, '이니테크',      'us-west', '2025-03-19'),
    (4, '엄브렐라',      'ap-south','2025-04-02'),
    (5, '웨인 엔터',     'us-east', '2025-05-27');

INSERT INTO orders (order_id, customer_id, amount, status, order_ts) VALUES
    -- 정상 거래
    (1001, 1,   299.00, 'completed', '2025-06-01 09:14:00'),
    (1002, 2,   149.00, 'completed', '2025-06-01 11:02:00'),
    (1003, 3,    89.00, 'completed', '2025-06-02 08:45:00'),
    (1004, 4,   450.00, 'completed', '2025-06-02 16:30:00'),
    (1005, 5,   199.00, 'completed', '2025-06-03 10:05:00'),
    (1006, 1,   299.00, 'completed', '2025-06-05 14:20:00'),
    (1007, 2,   500.00, 'completed', '2025-06-06 12:00:00'),

    -- 이상치 1: 중복 결제 — 같은 고객, 같은 금액, 12초 간격
    (1008, 3,   780.00, 'completed', '2025-06-07 13:41:05'),
    (1009, 3,   780.00, 'completed', '2025-06-07 13:41:17'),

    -- 이상치 2: 'completed' 상태로 남아 있는 음수 금액 (환불이어야 정상)
    (1010, 4, -1500.00, 'completed', '2025-06-08 09:00:00'),

    -- 이상치 3: 정상 범위를 크게 벗어난 극단 이상치
    (1011, 5, 999999.00, 'completed', '2025-06-09 03:12:00');
