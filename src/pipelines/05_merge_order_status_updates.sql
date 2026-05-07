-- 08_merge_order_status_updates.sql
--
--- staging_orders의 주문 상태 이벤트를 processed_orders에 MERGE INTO로 반영한다.
--
-- 목적:
-- - raw_orders는 주문 상태 이벤트를 append-only로 보관한다.
-- - staging_orders는 raw_orders를 파싱한 정제 이벤트 로그다.
-- - processed_orders는 order_id 기준 최신 주문 상태만 관리한다.
--
-- Key: order_id
--
-- 결정 사항:
--   1. 같은 order_id에 대해 NEW, PARTIALLY_FILLED, FILLED, CANCELED 이벤트가 여러 번 존재할 수 있다.
--   2. MERGE source 안에서 order_id별 최신 이벤트만 남기기 위해 MERGE 직전 dedup을 수행한다.
--   3. dedup 기준:
--        - PARTITION BY order_id
--        - ORDER BY event_time DESC, status_rank DESC, source_offset DESC
--   4. late event 방어:
--        - source.event_time >= target.updated_at 인 경우에만 UPDATE한다.
--
-- 상태 우선순위:
--   NEW              = 1
--   PARTIALLY_FILLED = 2
--   FILLED           = 3
--   CANCELED         = 3

MERGE INTO glue.binance_lakehouse.processed_orders AS target
USING (
    SELECT
        order_id,
        client_id,
        symbol,
        side,
        order_type,
        order_price,
        order_qty,
        filled_qty,
        avg_fill_price,
        event_type,
        order_status,
        created_at,
        event_time AS updated_at,
        CASE
            WHEN order_status = 'FILLED' THEN event_time
            ELSE NULL
        END AS filled_at,
        CASE
            WHEN order_status = 'CANCELED' THEN event_time
            ELSE NULL
        END AS canceled_at,
        simulated_parameters,
        source_topic,
        source_partition,
        source_offset
    FROM (
        SELECT
            order_id,
            client_id,
            symbol,
            side,
            order_type,
            order_price,
            order_qty,
            filled_qty,
            avg_fill_price,
            event_type,
            order_status,
            event_time,
            MIN(event_time) OVER (
                PARTITION BY order_id
            ) AS created_at,
            simulated_parameters,
            source_topic,
            source_partition,
            source_offset,
            CASE
                WHEN order_status = 'NEW' THEN 1
                WHEN order_status = 'PARTIALLY_FILLED' THEN 2
                WHEN order_status IN ('FILLED', 'CANCELED') THEN 3
                ELSE 0
            END AS status_rank,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY
                    event_time DESC,
                    CASE
                        WHEN order_status = 'NEW' THEN 1
                        WHEN order_status = 'PARTIALLY_FILLED' THEN 2
                        WHEN order_status IN ('FILLED', 'CANCELED') THEN 3
                        ELSE 0
                    END DESC,
                    source_offset DESC
            ) AS rn
        FROM glue.binance_lakehouse.staging_orders
        WHERE order_id IS NOT NULL
          AND order_status IS NOT NULL
          AND event_time IS NOT NULL
    ) deduped
    WHERE rn = 1
) AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET
    target.client_id             = source.client_id,
    target.symbol                = source.symbol,
    target.side                  = source.side,
    target.order_type            = source.order_type,
    target.order_price           = source.order_price,
    target.order_qty             = source.order_qty,
    target.filled_qty            = source.filled_qty,
    target.avg_fill_price        = source.avg_fill_price,
    target.event_type            = source.event_type,
    target.order_status          = source.order_status,
    target.created_at            = LEAST(target.created_at, source.created_at),
    target.updated_at            = source.updated_at,
    target.filled_at             = CASE
                                      WHEN source.order_status = 'FILLED'
                                      THEN source.filled_at
                                      ELSE target.filled_at
                                   END,
    target.canceled_at           = CASE
                                      WHEN source.order_status = 'CANCELED'
                                      THEN source.canceled_at
                                      ELSE target.canceled_at
                                   END,
    target.simulated_parameters  = source.simulated_parameters,
    target.source_topic          = source.source_topic,
    target.source_partition      = source.source_partition,
    target.source_offset         = source.source_offset

WHEN NOT MATCHED THEN INSERT (
    order_id,
    client_id,
    symbol,
    side,
    order_type,
    order_price,
    order_qty,
    filled_qty,
    avg_fill_price,
    event_type,
    order_status,
    created_at,
    updated_at,
    filled_at,
    canceled_at,
    simulated_parameters,
    source_topic,
    source_partition,
    source_offset
)
VALUES (
    source.order_id,
    source.client_id,
    source.symbol,
    source.side,
    source.order_type,
    source.order_price,
    source.order_qty,
    source.filled_qty,
    source.avg_fill_price,
    source.event_type,
    source.order_status,
    source.created_at,
    source.updated_at,
    source.filled_at,
    source.canceled_at,
    source.simulated_parameters,
    source.source_topic,
    source.source_partition,
    source.source_offset
);