"""07_build_order_execution_summary_window.py

processed_orders -> order_execution_summary MERGE job.

Affected summary keys are selected by ingest_time. The summary hour itself stays
on business time: order created_at.
"""

from __future__ import annotations

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import (
    PROCESSED_ORDERS,
    ORDER_EXECUTION_SUMMARY,
    MARKET_HOURLY_SUMMARY,
)


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_order_execution_summary_window")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW order_execution_summary_source AS
        WITH affected_summary_keys AS (
            SELECT DISTINCT
                date_trunc('hour', created_at) AS summary_hour,
                symbol
            FROM {PROCESSED_ORDERS}
            WHERE to_timestamp(ingest_time) >= TIMESTAMP '{args.start_ts}'
              AND to_timestamp(ingest_time) < TIMESTAMP '{args.end_ts}'
              AND created_at IS NOT NULL
              AND symbol IS NOT NULL
        ),
        order_hourly AS (
            SELECT
                keys.summary_hour,
                keys.symbol,

                CAST(count(*) AS BIGINT) AS total_orders,

                CAST(sum(CASE WHEN p.order_status = 'FILLED' THEN 1 ELSE 0 END) AS BIGINT) AS filled_orders,
                CAST(sum(CASE WHEN p.order_status = 'CANCELED' THEN 1 ELSE 0 END) AS BIGINT) AS canceled_orders,

                CAST(sum(CASE WHEN p.order_status = 'FILLED' THEN 1 ELSE 0 END) AS DOUBLE)
                    / CAST(count(*) AS DOUBLE) AS fill_rate,

                CAST(sum(CASE WHEN p.order_status = 'CANCELED' THEN 1 ELSE 0 END) AS DOUBLE)
                    / CAST(count(*) AS DOUBLE) AS cancel_rate,

                CAST(avg(
                    CASE
                        WHEN p.order_status = 'FILLED'
                             AND p.filled_at IS NOT NULL
                             AND p.created_at IS NOT NULL
                        THEN unix_timestamp(p.filled_at) - unix_timestamp(p.created_at)
                        ELSE NULL
                    END
                ) AS DOUBLE) AS avg_fill_delay_sec,

                CAST(avg(p.order_qty) AS DECIMAL(20, 8)) AS avg_order_qty,
                CAST(avg(p.filled_qty) AS DECIMAL(20, 8)) AS avg_filled_qty,
                CAST(sum(p.order_qty) AS DECIMAL(30, 8)) AS total_order_qty,
                CAST(sum(p.filled_qty) AS DECIMAL(30, 8)) AS total_filled_qty,

                -- Phase X: 방향 분리 체결가중 평균 체결가 (FILLED 주문만, filled_qty 가중)
                CAST(
                    sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'BUY'
                             THEN p.filled_qty * p.avg_fill_price END)
                    / NULLIF(sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'BUY'
                             THEN p.filled_qty END), 0)
                    AS DECIMAL(20, 8)
                ) AS avg_buy_fill_price,
                CAST(
                    sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'SELL'
                             THEN p.filled_qty * p.avg_fill_price END)
                    / NULLIF(sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'SELL'
                             THEN p.filled_qty END), 0)
                    AS DECIMAL(20, 8)
                ) AS avg_sell_fill_price,

                -- 방향별 체결량 (slippage_cost_quote 금액 환산용)
                CAST(sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'BUY'
                         THEN p.filled_qty ELSE 0 END) AS DECIMAL(30, 8)) AS buy_filled_qty,
                CAST(sum(CASE WHEN p.order_status = 'FILLED' AND p.side = 'SELL'
                         THEN p.filled_qty ELSE 0 END) AS DECIMAL(30, 8)) AS sell_filled_qty
            FROM affected_summary_keys keys
            JOIN {PROCESSED_ORDERS} p
              ON date_trunc('hour', p.created_at) = keys.summary_hour
             AND p.symbol = keys.symbol
            GROUP BY keys.summary_hour, keys.symbol
        )
        SELECT
            keys.summary_hour AS summary_hour,
            keys.symbol AS symbol,
            o.total_orders AS total_orders,
            o.filled_orders AS filled_orders,
            o.canceled_orders AS canceled_orders,
            o.fill_rate AS fill_rate,
            o.cancel_rate AS cancel_rate,
            o.avg_fill_delay_sec AS avg_fill_delay_sec,
            o.avg_order_qty AS avg_order_qty,
            o.avg_filled_qty AS avg_filled_qty,
            o.total_order_qty AS total_order_qty,
            o.total_filled_qty AS total_filled_qty,

            -- Phase X: 벤치마크(VWAP) 대비 방향 분리 슬리피지 (양수=유리)
            -- 순환 방지: benchmark_vwap 은 market_hourly_summary(시장 체결) 에서 오고,
            -- 체결가는 processed_orders(주문 앵커=분 close 기반) 에서 온다. 서로 다른 시계열.
            m.vwap AS benchmark_vwap,
            o.avg_buy_fill_price AS avg_buy_fill_price,
            o.avg_sell_fill_price AS avg_sell_fill_price,
            (m.vwap - o.avg_buy_fill_price) / NULLIF(m.vwap, 0) * 10000 AS buy_slippage_bps,
            (o.avg_sell_fill_price - m.vwap) / NULLIF(m.vwap, 0) * 10000 AS sell_slippage_bps,
            CAST(
                COALESCE(o.buy_filled_qty * (m.vwap - o.avg_buy_fill_price), 0)
                + COALESCE(o.sell_filled_qty * (o.avg_sell_fill_price - m.vwap), 0)
                AS DECIMAL(30, 8)
            ) AS slippage_cost_quote,

            current_timestamp() AS updated_at
        FROM affected_summary_keys keys
        LEFT JOIN order_hourly o
          ON o.summary_hour = keys.summary_hour
         AND o.symbol = keys.symbol
        LEFT JOIN {MARKET_HOURLY_SUMMARY} m
          ON m.summary_hour = keys.summary_hour
         AND m.symbol = keys.symbol
    """)

    spark.sql(f"""
        MERGE INTO {ORDER_EXECUTION_SUMMARY} AS target
        USING order_execution_summary_source AS source
        ON target.summary_hour = source.summary_hour
           AND target.symbol = source.symbol

        WHEN MATCHED THEN UPDATE SET
            target.total_orders = source.total_orders,
            target.filled_orders = source.filled_orders,
            target.canceled_orders = source.canceled_orders,
            target.fill_rate = source.fill_rate,
            target.cancel_rate = source.cancel_rate,
            target.avg_fill_delay_sec = source.avg_fill_delay_sec,
            target.avg_order_qty = source.avg_order_qty,
            target.avg_filled_qty = source.avg_filled_qty,
            target.total_order_qty = source.total_order_qty,
            target.total_filled_qty = source.total_filled_qty,
            target.benchmark_vwap = source.benchmark_vwap,
            target.avg_buy_fill_price = source.avg_buy_fill_price,
            target.avg_sell_fill_price = source.avg_sell_fill_price,
            target.buy_slippage_bps = source.buy_slippage_bps,
            target.sell_slippage_bps = source.sell_slippage_bps,
            target.slippage_cost_quote = source.slippage_cost_quote,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
    """)

    source_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM order_execution_summary_source"
    ).collect()[0]["cnt"]

    target_total = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {ORDER_EXECUTION_SUMMARY}"
    ).collect()[0]["cnt"]

    print(
        "[phase3_build_order_execution_summary_window] complete "
        f"run_id={args.run_id}, "
        f"source_rows={source_count}, "
        f"target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
