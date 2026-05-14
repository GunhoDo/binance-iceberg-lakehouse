"""08_check_data_quality.py

Phase3 data quality check job.

검사 항목:
- 주요 table row count
- business key duplicate count
- 핵심 key null count

결과:
- glue.binance_lakehouse.data_quality_summary append
"""

from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)
from pyspark.sql import functions as F

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import (
    PROCESSED_TRADES,
    PROCESSED_KLINES,
    PROCESSED_ORDERS,
    MARKET_HOURLY_SUMMARY,
    ORDER_EXECUTION_SUMMARY,
    DATA_QUALITY_SUMMARY,
)


def scalar(spark, query: str):
    return spark.sql(query).collect()[0][0]


def window_where(time_column: str, start_ts: str, end_ts: str) -> str:
    return (
        f"{time_column} >= to_timestamp('{start_ts}') "
        f"AND {time_column} < to_timestamp('{end_ts}')"
    )


def filtered_table(table_name: str, time_column: str, start_ts: str, end_ts: str) -> str:
    return f"(SELECT * FROM {table_name} WHERE {window_where(time_column, start_ts, end_ts)}) AS windowed"


def count_query(table_name: str, time_column: str, start_ts: str, end_ts: str) -> str:
    return f"SELECT COUNT(*) FROM {filtered_table(table_name, time_column, start_ts, end_ts)}"


def null_count_query(
    table_name: str,
    time_column: str,
    start_ts: str,
    end_ts: str,
    null_condition: str,
) -> str:
    return f"""
        SELECT COUNT(*)
        FROM {filtered_table(table_name, time_column, start_ts, end_ts)}
        WHERE {null_condition}
        """


def duplicate_count_query(
    table_name: str,
    time_column: str,
    start_ts: str,
    end_ts: str,
    distinct_expr: str,
) -> str:
    return f"""
        SELECT COUNT(*) - COUNT(DISTINCT {distinct_expr})
        FROM {filtered_table(table_name, time_column, start_ts, end_ts)}
        """


def add_result(
    results: list[Row],
    run_id: str,
    table_name: str,
    check_name: str,
    row_count: int,
    null_count: int,
    duplicate_count: int,
    warning_message: str | None = None,
) -> None:
    status = "PASS"
    if null_count > 0 or duplicate_count > 0:
        status = "FAIL"

    results.append(
        Row(
            run_id=run_id,
            checked_at=None,
            table_name=table_name,
            check_name=check_name,
            check_status=status,
            row_count=int(row_count),
            null_count=int(null_count),
            duplicate_count=int(duplicate_count),
            warning_message=warning_message,
        )
    )


def run() -> None:
    RESULT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("checked_at", TimestampType(), True),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_status", StringType(), False),
    StructField("row_count", LongType(), False),
    StructField("null_count", LongType(), False),
    StructField("duplicate_count", LongType(), False),
    StructField("warning_message", StringType(), True),
    ])

    args = parse_job_args()
    spark = get_spark("phase3_check_data_quality")

    results: list[Row] = []
    window_message = f"window=[{args.start_ts}, {args.end_ts})"

    # -------------------------------------------------------------------------
    # processed_trades
    # -------------------------------------------------------------------------
    row_count = scalar(spark, count_query(PROCESSED_TRADES, "trade_time", args.start_ts, args.end_ts))
    null_count = scalar(
        spark,
        null_count_query(PROCESSED_TRADES, "trade_time", args.start_ts, args.end_ts, "trade_id IS NULL"),
    )
    duplicate_count = scalar(
        spark,
        duplicate_count_query(PROCESSED_TRADES, "trade_time", args.start_ts, args.end_ts, "trade_id"),
    )
    add_result(
        results,
        args.run_id,
        "processed_trades",
        "trade_id_not_null_and_unique",
        row_count,
        null_count,
        duplicate_count,
        window_message,
    )

    # -------------------------------------------------------------------------
    # processed_klines
    # -------------------------------------------------------------------------
    row_count = scalar(spark, count_query(PROCESSED_KLINES, "open_time", args.start_ts, args.end_ts))
    null_count = scalar(
        spark,
        null_count_query(
            PROCESSED_KLINES,
            "open_time",
            args.start_ts,
            args.end_ts,
            "symbol IS NULL OR `interval` IS NULL OR open_time IS NULL",
        ),
    )
    duplicate_count = scalar(
        spark,
        duplicate_count_query(
            PROCESSED_KLINES,
            "open_time",
            args.start_ts,
            args.end_ts,
            "concat_ws(':', symbol, `interval`, cast(open_time AS string))",
        ),
    )
    add_result(
        results,
        args.run_id,
        "processed_klines",
        "symbol_interval_open_time_not_null_and_unique",
        row_count,
        null_count,
        duplicate_count,
        window_message,
    )

    # -------------------------------------------------------------------------
    # processed_orders
    # -------------------------------------------------------------------------
    row_count = scalar(spark, count_query(PROCESSED_ORDERS, "updated_at", args.start_ts, args.end_ts))
    null_count = scalar(
        spark,
        null_count_query(PROCESSED_ORDERS, "updated_at", args.start_ts, args.end_ts, "order_id IS NULL"),
    )
    duplicate_count = scalar(
        spark,
        duplicate_count_query(PROCESSED_ORDERS, "updated_at", args.start_ts, args.end_ts, "order_id"),
    )
    add_result(
        results,
        args.run_id,
        "processed_orders",
        "order_id_not_null_and_unique",
        row_count,
        null_count,
        duplicate_count,
        window_message,
    )

    # -------------------------------------------------------------------------
    # market_hourly_summary
    # -------------------------------------------------------------------------
    row_count = scalar(spark, count_query(MARKET_HOURLY_SUMMARY, "summary_hour", args.start_ts, args.end_ts))
    null_count = scalar(
        spark,
        null_count_query(
            MARKET_HOURLY_SUMMARY,
            "summary_hour",
            args.start_ts,
            args.end_ts,
            "symbol IS NULL OR summary_hour IS NULL",
        ),
    )
    duplicate_count = scalar(
        spark,
        duplicate_count_query(
            MARKET_HOURLY_SUMMARY,
            "summary_hour",
            args.start_ts,
            args.end_ts,
            "concat_ws(':', symbol, cast(summary_hour AS string))",
        ),
    )
    add_result(
        results,
        args.run_id,
        "market_hourly_summary",
        "symbol_summary_hour_not_null_and_unique",
        row_count,
        null_count,
        duplicate_count,
        window_message,
    )

    # -------------------------------------------------------------------------
    # order_execution_summary
    # -------------------------------------------------------------------------
    row_count = scalar(spark, count_query(ORDER_EXECUTION_SUMMARY, "summary_hour", args.start_ts, args.end_ts))
    null_count = scalar(
        spark,
        null_count_query(
            ORDER_EXECUTION_SUMMARY,
            "summary_hour",
            args.start_ts,
            args.end_ts,
            "symbol IS NULL OR summary_hour IS NULL",
        ),
    )
    duplicate_count = scalar(
        spark,
        duplicate_count_query(
            ORDER_EXECUTION_SUMMARY,
            "summary_hour",
            args.start_ts,
            args.end_ts,
            "concat_ws(':', symbol, cast(summary_hour AS string))",
        ),
    )
    add_result(
        results,
        args.run_id,
        "order_execution_summary",
        "symbol_summary_hour_not_null_and_unique",
        row_count,
        null_count,
        duplicate_count,
        window_message,
    )

    result_df = spark.createDataFrame(results, schema=RESULT_SCHEMA).withColumn(
        "checked_at",
        F.current_timestamp(),
    )

    # 컬럼 순서를 target table과 맞춤
    result_df = result_df.select(
        "run_id",
        "checked_at",
        "table_name",
        "check_name",
        "check_status",
        "row_count",
        "null_count",
        "duplicate_count",
        "warning_message",
    )

    result_df.writeTo(DATA_QUALITY_SUMMARY).append()

    print(
        "[phase3_check_data_quality] complete "
        f"run_id={args.run_id}, checks={len(results)}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
