"""06_build_market_hourly_summary_window.py

processed_klines + processed_trades -> market_hourly_summary window 기반 MERGE job.

멱등성 기준:
- (summary_hour, symbol) 기준 MERGE
- source view 컬럼 순서를 target table과 맞춘 뒤 INSERT * 사용
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
        WITH kline_hourly AS (
            SELECT
                date_trunc('hour', open_time) AS summary_hour,
                symbol,

                first(open, true) AS open_price,
                max(high) AS high_price,
                min(low) AS low_price,
                last(close, true) AS close_price,

                CAST(sum(volume) AS DECIMAL(30, 8)) AS kline_volume,
                CAST(sum(quote_volume) AS DECIMAL(30, 8)) AS kline_quote_volume,
                CAST(sum(number_of_trades) AS BIGINT) AS kline_number_of_trades
            FROM {PROCESSED_KLINES}
            WHERE open_time >= TIMESTAMP '{args.start_ts}'
              AND open_time < TIMESTAMP '{args.end_ts}'
            GROUP BY date_trunc('hour', open_time), symbol
        ),
        trade_hourly AS (
            SELECT
                date_trunc('hour', trade_time) AS summary_hour,
                symbol,

                CAST(count(*) AS BIGINT) AS trade_count,
                CAST(sum(qty) AS DECIMAL(30, 8)) AS trade_qty,
                CAST(sum(quote_qty) AS DECIMAL(30, 8)) AS trade_quote_qty,
                CAST(avg(price) AS DECIMAL(20, 8)) AS avg_trade_price,

                CAST(sum(CASE WHEN is_buyer_maker = true THEN 1 ELSE 0 END) AS BIGINT) AS maker_trade_count,
                CAST(sum(CASE WHEN is_buyer_maker = false THEN 1 ELSE 0 END) AS BIGINT) AS taker_trade_count
            FROM {PROCESSED_TRADES}
            WHERE trade_time >= TIMESTAMP '{args.start_ts}'
              AND trade_time < TIMESTAMP '{args.end_ts}'
            GROUP BY date_trunc('hour', trade_time), symbol
        )
        SELECT
            COALESCE(k.summary_hour, t.summary_hour) AS summary_hour,
            COALESCE(k.symbol, t.symbol) AS symbol,

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
        FROM kline_hourly k
        FULL OUTER JOIN trade_hourly t
          ON k.summary_hour = t.summary_hour
         AND k.symbol = t.symbol
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