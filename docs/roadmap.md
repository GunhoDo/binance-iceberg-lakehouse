# Roadmap

PRD §17과 동일한 Phase 분할을 따른다.

## Phase 0. Study & Design

- Binance market data 구조 학습
- trade / kline / order event 차이 정리
- order simulator 설계
- topic 설계
- raw / processed / serving table 설계
- MVP 범위 결정

## Phase 1. Kafka + Raw Zone MVP

- `trades` collector 구현
- `klines` collector 구현
- `orders` simulator 구현
- Kafka topic 생성
- Spark Structured Streaming으로 raw event 적재
- Raw Zone 재처리 가능성 확인

## Phase 2. Iceberg Core MVP

- `processed_trades` 구현
- `processed_klines` 구현
- `processed_orders` 구현
- kline update MERGE 구현
- order status MERGE 구현
- `market_hourly_summary` 구현
- `order_execution_summary` 구현
- Snapshot 및 metadata table 확인
- Compaction 전후 비교

## Phase 3. Observability + Airflow

- `data_quality_summary` 생성
- `pipeline_run_summary` 생성
- `table_health_summary` 생성
- Daily pipeline DAG 구현
- Maintenance DAG 구현

## Phase 4. QuickSight Dashboard

- Market metrics 시각화
- Order execution metrics 시각화
- Data quality metrics 시각화
- Iceberg operation metrics 시각화

## Phase 5. Maintenance

- 코드 리팩토링
- 검증 쿼리 추가
- 실행 문서 보완
- 결과 재측정

## Phase 진입 조건

각 Phase는 PRD §18의 Success Criteria 항목 중 해당 Phase 항목이 모두 충족됐을 때
다음 Phase로 진입한다. Phase가 끝나기 전에 다음 Phase 코드를 추가하지 않는다.
