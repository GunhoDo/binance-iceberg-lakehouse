"""07_build_order_execution_summary_window.py

processed_orders -> order_execution_summary window 기반 MERGE job.

멱등성 기준:
- (summary_hour, symbol) 기준 MERGE
- source view 컬럼 순서를 target table과 맞춘 뒤 INSERT * 사용
"""

from __future__ import annotations

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import PROCESSED_ORDERS, ORDER_EXECUTION_SUMMARY


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_order_execution_summary_window")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW order_execution_summary_source AS
        SELECT
            date_trunc('hour', created_at) AS summary_hour,
            symbol,

            CAST(count(*) AS BIGINT) AS total_orders,

            CAST(sum(CASE WHEN order_status = 'FILLED' THEN 1 ELSE 0 END) AS BIGINT) AS filled_orders,
            CAST(sum(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END) AS BIGINT) AS canceled_orders,

            CAST(sum(CASE WHEN order_status = 'FILLED' THEN 1 ELSE 0 END) AS DOUBLE)
                / CAST(count(*) AS DOUBLE) AS fill_rate,

            CAST(sum(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END) AS DOUBLE)
                / CAST(count(*) AS DOUBLE) AS cancel_rate,

            CAST(avg(
                CASE
                    WHEN order_status = 'FILLED'
                         AND filled_at IS NOT NULL
                         AND created_at IS NOT NULL
                    THEN unix_timestamp(filled_at) - unix_timestamp(created_at)
                    ELSE NULL
                END
            ) AS DOUBLE) AS avg_fill_delay_sec,

            CAST(avg(order_qty) AS DECIMAL(20, 8)) AS avg_order_qty,
            CAST(avg(filled_qty) AS DECIMAL(20, 8)) AS avg_filled_qty,
            CAST(sum(order_qty) AS DECIMAL(30, 8)) AS total_order_qty,
            CAST(sum(filled_qty) AS DECIMAL(30, 8)) AS total_filled_qty,

            current_timestamp() AS updated_at
        FROM {PROCESSED_ORDERS}
        WHERE created_at >= TIMESTAMP '{args.start_ts}'
          AND created_at < TIMESTAMP '{args.end_ts}'
          AND created_at IS NOT NULL
          AND symbol IS NOT NULL
        GROUP BY date_trunc('hour', created_at), symbol
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