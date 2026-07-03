"""Benchmark 모듈 (PRD v2 P2/P3).

통제 리플레이 부하 위에서 스트리밍 end-to-end lag를 측정(P2)하고 레버별로 분해(P3)한다.
production 파이프라인(src/pipelines, src/jobs)과 분리된 벤치 전용 코드다.

- spark_bench.py : 로컬 Iceberg(hadoop catalog) SparkSession — AWS 불요.
- lag_stream.py  : Kafka trades → 로컬 Iceberg, commit_ts−produce_ts 샘플 적재.
- lag_report.py  : lag 샘플 → p50/p95/p99 + throughput 표.
"""
