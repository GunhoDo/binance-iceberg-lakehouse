"""build_market_hourly_summary.py

processed_klines + processed_trades → market_hourly_summary 사전 집계.

PRD §10.6 참조.

설계 노트:
- processed_klines 는 OHLCV 지표의 기준이고, processed_trades 는 trade count,
  average trade size 등 보조 지표를 제공한다 (PRD §10.6).
- 두 processed table을 serving 단계에서 symbol과 time window 기준으로 조합한다.
- Write Pattern: MERGE, Incremental (PRD §9).

processed_klines + processed_trades → market_hourly_summary serving table.

symbol/hour 단위 market KPI를 생성한다.

실행:
    PYTHONPATH=. spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.parquet.enableVectorizedReader=false \
        --conf spark.sql.iceberg.vectorization.enabled=false \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.driver.memory=3g \
        src/pipelines/06_build_market_hourly_summary.py
"""
from __future__ import annotations

from src.pipelines.common.spark_session import get_spark

TRADES_TABLE = "glue.binance_lakehouse.processed_trades"
KLINES_TABLE = "glue.binance_lakehouse.processed_klines"
TARGET_TABLE = "glue.binance_lakehouse.market_hourly_summary"


def run() -> None:
    spark = get_spark("build_market_hourly_summary")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW kline_hourly AS
        SELECT
            date_trunc('hour', open_time) AS summary_hour,
            symbol,

            CAST(min_by(open, open_time) AS DECIMAL(20, 8)) AS open_price,
            CAST(MAX(high) AS DECIMAL(20, 8)) AS high_price,
            CAST(MIN(low) AS DECIMAL(20, 8)) AS low_price,
            CAST(max_by(close, open_time) AS DECIMAL(20, 8)) AS close_price,

            CAST(SUM(volume) AS DECIMAL(30, 8)) AS kline_volume,
            CAST(SUM(quote_volume) AS DECIMAL(30, 8)) AS kline_quote_volume,
            SUM(number_of_trades) AS kline_number_of_trades
        FROM {KLINES_TABLE}
        WHERE open_time IS NOT NULL
          AND symbol IS NOT NULL
        GROUP BY date_trunc('hour', open_time), symbol
    """)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW trade_hourly AS
        SELECT
            date_trunc('hour', trade_time) AS summary_hour,
            symbol,

            COUNT(*) AS trade_count,
            CAST(SUM(qty) AS DECIMAL(30, 8)) AS trade_qty,
            CAST(SUM(quote_qty) AS DECIMAL(30, 8)) AS trade_quote_qty,
            CAST(AVG(price) AS DECIMAL(20, 8)) AS avg_trade_price,

            SUM(CASE WHEN is_buyer_maker = true THEN 1 ELSE 0 END) AS maker_trade_count,
            SUM(CASE WHEN is_buyer_maker = false THEN 1 ELSE 0 END) AS taker_trade_count
        FROM {TRADES_TABLE}
        WHERE trade_time IS NOT NULL
          AND symbol IS NOT NULL
        GROUP BY date_trunc('hour', trade_time), symbol
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW market_hourly_summary_source AS
        SELECT
            COALESCE(k.summary_hour, t.summary_hour) AS summary_hour,
            COALESCE(k.symbol, t.symbol) AS symbol,

            k.open_price,
            k.high_price,
            k.low_price,
            k.close_price,

            k.kline_volume,
            k.kline_quote_volume,
            k.kline_number_of_trades,

            COALESCE(t.trade_count, 0) AS trade_count,
            COALESCE(t.trade_qty, CAST(0 AS DECIMAL(30, 8))) AS trade_qty,
            COALESCE(t.trade_quote_qty, CAST(0 AS DECIMAL(30, 8))) AS trade_quote_qty,
            t.avg_trade_price,
            COALESCE(t.maker_trade_count, 0) AS maker_trade_count,
            COALESCE(t.taker_trade_count, 0) AS taker_trade_count,

            current_timestamp() AS updated_at
        FROM kline_hourly k
        FULL OUTER JOIN trade_hourly t
          ON k.summary_hour = t.summary_hour
         AND k.symbol = t.symbol
    """)

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING market_hourly_summary_source AS source
        ON target.summary_hour = source.summary_hour
           AND target.symbol = source.symbol

        WHEN MATCHED THEN UPDATE SET
            target.open_price             = source.open_price,
            target.high_price             = source.high_price,
            target.low_price              = source.low_price,
            target.close_price            = source.close_price,
            target.kline_volume           = source.kline_volume,
            target.kline_quote_volume     = source.kline_quote_volume,
            target.kline_number_of_trades = source.kline_number_of_trades,
            target.trade_count            = source.trade_count,
            target.trade_qty              = source.trade_qty,
            target.trade_quote_qty        = source.trade_quote_qty,
            target.avg_trade_price        = source.avg_trade_price,
            target.maker_trade_count      = source.maker_trade_count,
            target.taker_trade_count      = source.taker_trade_count,
            target.updated_at             = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE}").collect()[0]["cnt"]
    print(f"market_hourly_summary build 완료: {count} rows")


if __name__ == "__main__":
    run()