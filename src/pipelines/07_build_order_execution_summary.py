"""build_order_execution_summary.py

processed_orders → order_execution_summary 사전 집계.

PRD §10.6 참조.

지표:
- total / filled / canceled / partial filled orders
- fill rate, cancel rate
- average fill delay
- average slippage proxy

Write Pattern: MERGE, Incremental (PRD §9).

주문 체결/취소 결과를 symbol/hour 단위로 집계한다.

실행:
    PYTHONPATH=. spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      src/pipelines/build_order_execution_summary.py
"""
from __future__ import annotations

from src.pipelines.common.spark_session import get_spark

SOURCE_TABLE = "glue.binance_lakehouse.processed_orders"
TARGET_TABLE = "glue.binance_lakehouse.order_execution_summary"


def run() -> None:
    spark = get_spark("build_order_execution_summary")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW order_execution_summary_source AS
        SELECT
            date_trunc('hour', created_at) AS summary_hour,
            symbol,

            COUNT(*) AS total_orders,

            SUM(CASE WHEN order_status = 'FILLED' THEN 1 ELSE 0 END) AS filled_orders,
            SUM(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled_orders,

            SUM(CASE WHEN order_status = 'FILLED' THEN 1 ELSE 0 END) / COUNT(*) AS fill_rate,
            SUM(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END) / COUNT(*) AS cancel_rate,

            AVG(
                CASE
                    WHEN order_status = 'FILLED'
                    THEN unix_timestamp(filled_at) - unix_timestamp(created_at)
                    ELSE NULL
                END
            ) AS avg_fill_delay_sec,

            CAST(AVG(order_qty) AS DECIMAL(20, 8)) AS avg_order_qty,
            CAST(AVG(filled_qty) AS DECIMAL(20, 8)) AS avg_filled_qty,
            CAST(SUM(order_qty) AS DECIMAL(30, 8)) AS total_order_qty,
            CAST(SUM(filled_qty) AS DECIMAL(30, 8)) AS total_filled_qty,

            current_timestamp() AS updated_at
        FROM {SOURCE_TABLE}
        WHERE created_at IS NOT NULL
          AND symbol IS NOT NULL
        GROUP BY date_trunc('hour', created_at), symbol
    """)

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING order_execution_summary_source AS source
        ON target.summary_hour = source.summary_hour
           AND target.symbol = source.symbol

        WHEN MATCHED THEN UPDATE SET
            target.total_orders       = source.total_orders,
            target.filled_orders      = source.filled_orders,
            target.canceled_orders    = source.canceled_orders,
            target.fill_rate          = source.fill_rate,
            target.cancel_rate        = source.cancel_rate,
            target.avg_fill_delay_sec = source.avg_fill_delay_sec,
            target.avg_order_qty      = source.avg_order_qty,
            target.avg_filled_qty     = source.avg_filled_qty,
            target.total_order_qty    = source.total_order_qty,
            target.total_filled_qty   = source.total_filled_qty,
            target.updated_at         = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE}").collect()[0]["cnt"]
    print(f"order_execution_summary build 완료: {count} rows")


if __name__ == "__main__":
    run()