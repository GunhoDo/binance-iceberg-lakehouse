# tests

현재 테스트는 표준 라이브러리 `unittest` 기반 정적 정책 테스트다.
Spark/Iceberg 런타임 없이 PRD/Decision의 핵심 약속이 코드에 반영되어 있는지 빠르게 확인한다.

## 현재 테스트 범위

- Raw: plain Parquet 유지, `ingest_date=YYYY-MM-DD` 파티션 정책
- Daily jobs: window 기반 raw reader / data quality scan 정책
- MERGE: kline/order dedup 기준과 late event 방어 조건
- Idempotency: `trade_id`, Kafka offset 기반 재실행 안전성
- Observability: `data_quality_summary`, `table_health_summary` append-only table 정책
- Maintenance: MOR table 대상 delete-file rewrite 정책

## 향후 추가하면 좋은 테스트

- Phase 1: simulator 출력 schema, collector 응답 파싱
- Phase 2: Spark/Iceberg local 통합 테스트로 MERGE 결과 row 검증
- Phase 3: `09_check_table_health.py` 측정 결과 schema 검증

## 실행

```bash
python -m unittest discover tests
```
