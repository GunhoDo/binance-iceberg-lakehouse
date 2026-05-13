"""06_build_market_hourly_summary_window.py

processed_klines + processed_trades -> market_hourly_summary MERGE job.

Affected summary keys are selected by ingest_time. The summary hour itself stays
on business time: kline open_time and trade_time.
"""

from __future__ import annotations

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import (
    PROCESSED_KLINES,
    PROCESSED_TRADES,
    MARKET_HOURLY_SUMMARY,
)


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_market_hourly_summary_window")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW market_hourly_summary_source AS
        WITH affected_summary_keys AS (
            SELECT DISTINCT
                date_trunc('hour', open_time) AS summary_hour,
                symbol
            FROM {PROCESSED_KLINES}
            WHERE to_timestamp(ingest_time) >= TIMESTAMP '{args.start_ts}'
              AND to_timestamp(ingest_time) < TIMESTAMP '{args.end_ts}'
              AND open_time IS NOT NULL
              AND symbol IS NOT NULL

            UNION

            SELECT DISTINCT
                date_trunc('hour', trade_time) AS summary_hour,
                symbol
            FROM {PROCESSED_TRADES}
            WHERE to_timestamp(ingest_time) >= TIMESTAMP '{args.start_ts}'
              AND to_timestamp(ingest_time) < TIMESTAMP '{args.end_ts}'
              AND trade_time IS NOT NULL
              AND symbol IS NOT NULL
        ),
        kline_hourly AS (
            SELECT
                keys.summary_hour,
                keys.symbol,

                first(k.open, true) AS open_price,
                max(k.high) AS high_price,
                min(k.low) AS low_price,
                last(k.close, true) AS close_price,

                CAST(sum(k.volume) AS DECIMAL(30, 8)) AS kline_volume,
                CAST(sum(k.quote_volume) AS DECIMAL(30, 8)) AS kline_quote_volume,
                CAST(sum(k.number_of_trades) AS BIGINT) AS kline_number_of_trades
            FROM affected_summary_keys keys
            JOIN {PROCESSED_KLINES} k
              ON date_trunc('hour', k.open_time) = keys.summary_hour
             AND k.symbol = keys.symbol
            GROUP BY keys.summary_hour, keys.symbol
        ),
        trade_hourly AS (
            SELECT
                keys.summary_hour,
                keys.symbol,

                CAST(count(*) AS BIGINT) AS trade_count,
                CAST(sum(t.qty) AS DECIMAL(30, 8)) AS trade_qty,
                CAST(sum(t.quote_qty) AS DECIMAL(30, 8)) AS trade_quote_qty,
                CAST(avg(t.price) AS DECIMAL(20, 8)) AS avg_trade_price,

                CAST(sum(CASE WHEN t.is_buyer_maker = true THEN 1 ELSE 0 END) AS BIGINT) AS maker_trade_count,
                CAST(sum(CASE WHEN t.is_buyer_maker = false THEN 1 ELSE 0 END) AS BIGINT) AS taker_trade_count
            FROM affected_summary_keys keys
            JOIN {PROCESSED_TRADES} t
              ON date_trunc('hour', t.trade_time) = keys.summary_hour
             AND t.symbol = keys.symbol
            GROUP BY keys.summary_hour, keys.symbol
        )
        SELECT
            keys.summary_hour AS summary_hour,
            keys.symbol AS symbol,

            k.open_price AS open_price,
            k.high_price AS high_price,
            k.low_price AS low_price,
            k.close_price AS close_price,

            k.kline_volume AS kline_volume,
            k.kline_quote_volume AS kline_quote_volume,
            k.kline_number_of_trades AS kline_number_of_trades,

            t.trade_count AS trade_count,
            t.trade_qty AS trade_qty,
            t.trade_quote_qty AS trade_quote_qty,
            t.avg_trade_price AS avg_trade_price,
            t.maker_trade_count AS maker_trade_count,
            t.taker_trade_count AS taker_trade_count,

            current_timestamp() AS updated_at
        FROM affected_summary_keys keys
        LEFT JOIN kline_hourly k
          ON k.summary_hour = keys.summary_hour
         AND k.symbol = keys.symbol
        LEFT JOIN trade_hourly t
          ON t.summary_hour = keys.summary_hour
         AND t.symbol = keys.symbol
    """)

    spark.sql(f"""
        MERGE INTO {MARKET_HOURLY_SUMMARY} AS target
        USING market_hourly_summary_source AS source
        ON target.summary_hour = source.summary_hour
           AND target.symbol = source.symbol

        WHEN MATCHED THEN UPDATE SET
            target.open_price = source.open_price,
            target.high_price = source.high_price,
            target.low_price = source.low_price,
            target.close_price = source.close_price,
            target.kline_volume = source.kline_volume,
            target.kline_quote_volume = source.kline_quote_volume,
            target.kline_number_of_trades = source.kline_number_of_trades,
            target.trade_count = source.trade_count,
            target.trade_qty = source.trade_qty,
            target.trade_quote_qty = source.trade_quote_qty,
            target.avg_trade_price = source.avg_trade_price,
            target.maker_trade_count = source.maker_trade_count,
            target.taker_trade_count = source.taker_trade_count,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
    """)

    source_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM market_hourly_summary_source"
    ).collect()[0]["cnt"]

    target_total = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {MARKET_HOURLY_SUMMARY}"
    ).collect()[0]["cnt"]

    print(
        "[phase3_build_market_hourly_summary_window] complete "
        f"run_id={args.run_id}, "
        f"source_rows={source_count}, "
        f"target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
