# 최종 프로젝트 — Binance 거래소 Market Data Lakehouse

Binance 공개 시장 데이터(`trades`, `klines`)와 시뮬레이션 주문 이벤트(`orders`)를 이용해
Apache Iceberg 기반 Lakehouse를 구축하는 데이터 엔지니어링 프로젝트.

본 프로젝트는 거래 시스템이 아니라 **데이터 플랫폼 MVP**다.
실제 매매·자동매매·전략 추천은 Non-Goal이다 (`docs/PRD.md` §5).

---

## 1. 도메인 정의 + 핵심 KPI 3개

### 도메인

암호화폐 거래소 시장 데이터 플랫폼. Binance의 공개 market data(`trades`, `klines`)를
수집하고, user-level 주문 이벤트는 도메인 가정을 명시한 시뮬레이터로 생성한다.

거래소의 사용자별 주문은 본질적으로 private data이므로 실데이터인 척 가장하지 않는다.
이 사실은 PRD §2, `docs/decisions.md` D1, `docs/simulator_design.md`에 명시한다.

| Event | Source |
|---|---|
| `trades` | Binance public market data (실데이터) |
| `klines` | Binance public market data (실데이터) |
| `orders` | simulator (합성 데이터) |

### 핵심 KPI 3개

**① Fill Rate** — 전체 주문 중 체결된 주문 비율.
거래소 플랫폼의 유동성과 체결 품질을 나타내는 핵심 지표다.

**② Symbol/Hour 거래량** — 심볼별 시간 단위 체결 거래량.
시장 활성도와 피크 트래픽 예측의 기준이 된다.

**③ Pipeline Freshness Lag** — 마지막 raw 적재로부터 경과 시간.
데이터 플랫폼의 운영 상태를 나타내는 핵심 운영 지표다.

---

## 2. 전체 아키텍처

```
Binance Public Market Data
  ├── BTCUSDT trades CSV (data.binance.vision)
  └── BTCUSDT klines CSV (data.binance.vision)
          ↓ collectors/csv_to_kafka.py
    Kafka (Docker, KRaft)
    ├── topic: trades
    └── topic: klines
          ↓ Spark Structured Streaming
    S3 (s3a://binance-iceberg-lake/)
    ├── raw/trades/   ← plain Parquet, append-only
    └── raw/klines/   ← plain Parquet, append-only
          ↓ Spark Batch (Phase 2~)
    Iceberg (Glue Catalog)
    ├── processed_trades
    ├── processed_klines
    └── processed_orders
          ↓
    ├── market_hourly_summary
    └── order_execution_summary
          ↓
    QuickSight (Phase 4~)
```

**기술 스택**

| 역할 | 기술 |
|---|---|
| 메시지 큐 | Apache Kafka (Docker, KRaft) |
| 스트리밍 처리 | Spark Structured Streaming 3.5.5 |
| 배치 처리 | PySpark 3.5.5 |
| 테이블 포맷 | Apache Iceberg (format-version 2) |
| Catalog | AWS Glue |
| Storage | AWS S3 |
| 쿼리 | Amazon Athena |
| 오케스트레이션 | Apache Airflow (Phase 3~) |
| BI | Amazon QuickSight (Phase 4~) |

---

## 3. 메달리온 3계층 의사결정

본 프로젝트는 Bronze/Silver/Gold 대신 **Raw / Processed / Serving** 명칭을 사용한다.
레이어의 책임이 이름에서 바로 드러나도록 하기 위함이다.

| 본 프로젝트 | 메달리온 대응 |
|---|---|
| Raw | Bronze |
| Processed | Silver |
| Serving | Gold |

### 3-1. Raw (Bronze)

**책임**: Kafka event를 원본 그대로 S3에 append-only로 보관한다.

**설계 결정**:
- Iceberg를 쓰지 않는다. plain Parquet으로 S3에 저장한다.
- 이유: Raw는 재처리 기준 원본이다. Iceberg의 snapshot/MERGE가 필요 없다.
  스터디 원칙 — "브론즈의 스몰파일은 아이스버그 밖이라 건드리지 못한다" — 을 따른다.
- Kafka 메타데이터(topic, partition, offset)를 함께 보존해 재처리 가능성을 확보한다.
- message_value는 raw JSON 문자열 그대로 보관한다. 파싱은 processed의 책임이다.

**파티션**: `year / month` 기준

**테이블**: `raw_trades`, `raw_klines`, `raw_orders`

### 3-2. Processed (Silver)

**책임**: raw event를 파싱·정제하고, 상태 변화를 MERGE INTO로 관리한다.

**설계 결정**:
- Iceberg COW(Copy-on-Write)를 기본으로 한다. 구현 단순성과 snapshot 비교의 명확성이 이유다.
- `trades`와 `klines`를 하나의 wide table로 합치지 않는다. 두 이벤트는 분석 단위가
  다르고, 합치면 sparse union schema가 발생한다 (`docs/decisions.md` D4).
- `processed_klines`: `(symbol, interval, open_time)` 기준 MERGE로 최신 kline 상태를 관리한다.
- `processed_orders`: `order_id` 기준 MERGE로 주문 상태 변화를 관리한다.

**테이블**: `processed_trades`, `processed_klines`, `processed_orders`

### 3-3. Serving (Gold)

**책임**: 대시보드 조회를 위해 processed table을 사전 집계한다.

**설계 결정**:
- 대시보드가 매번 processed table 전체를 스캔하지 않도록 serving table로 사전 계산한다.
- `market_hourly_summary`: `processed_klines`(OHLCV 기준) + `processed_trades`(보조 지표) 조합.
- `order_execution_summary`: `processed_orders` 기반 주문 KPI 집계.

**테이블**: `market_hourly_summary`, `order_execution_summary`

---

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

### 주문 상태 변화 — MERGE INTO

주문 하나가 lifecycle 동안 `NEW → PARTIALLY_FILLED → FILLED` 또는 `NEW → CANCELED`로
상태가 바뀐다. 기존 Hive/Parquet 구조에서는 partition 전체를 재작성해야 한다.
Iceberg MERGE INTO는 변경된 row만 처리한다.

```sql
MERGE INTO glue.binance_lakehouse.processed_orders t
USING staging s ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Kline 반복 Update

Kline은 interval이 진행되는 동안 같은 `(symbol, interval, open_time)` 키로 수십 번
update가 도착한다. Raw에서는 전부 append하고, processed에서 MERGE로 최신 상태만 유지한다.
Parquet 구조였다면 partition 전체 재작성이 필요했다.

### Snapshot 기반 재처리

Raw Zone의 Kafka offset + Iceberg snapshot 조합으로 특정 시점 상태로 복구가 가능하다.
집계 로직 버그 발견 시 raw event를 기준으로 processed table을 재생성할 수 있다.

### Metadata Table 기반 운영

Iceberg metadata table(`snapshots`, `files`, `partitions`)을 SQL로 직접 조회해
운영 상태를 확인한다. 별도 모니터링 도구 없이 파일 수, snapshot 수, compaction
필요 여부를 파악한다.

```sql
SELECT COUNT(*) AS file_count,
       AVG(file_size_in_bytes) / 1024 / 1024 AS avg_file_size_mb
FROM glue.binance_lakehouse.processed_trades.files;
```

---

## 5. 운영 헬스 체크 쿼리 모음

> Phase 3 구현 후 `sql/health_queries/` 디렉토리에 추가한다.

TODO: Phase 3에서 아래 쿼리를 구현하고 결과 스크린샷을 추가한다.

- snapshot 수 및 최근 commit 시간
- 파일 수 및 평균 파일 크기
- small file count (64MB 미만)
- freshness lag (마지막 ingest_time 기준)
- raw vs processed row count 차이
- compaction 필요 여부 판단

---

## 6. 대시보드 (스크린샷 + 운영 메트릭)

> Phase 4 구현 후 스크린샷을 추가한다.

TODO: QuickSight 대시보드 구성 후 아래 항목의 스크린샷을 추가한다.

- Market Dashboard (OHLCV, 거래량, 변동성)
- Order Execution Dashboard (fill rate, cancel rate, slippage)
- Data Quality Dashboard (freshness lag, row count, duplicate)
- Iceberg Operations Dashboard (file count, snapshot count, compaction)

지표 정의는 `docs/quicksight_metrics.md` 참조.

---

## 7. 100x 스케일 아웃 시나리오

현재 MVP는 BTCUSDT 2024년 1~3월치, 일 약 400만 건(trades 기준) 수준이다.
100x(일 4억 건)가 되면 어디가 깨지고 어떻게 대응하는지를 설계 수준에서 정리한다.

### 깨지는 지점

| 컴포넌트 | 현재 | 100x 문제 |
|---|---|---|
| Kafka | 로컬 1 broker, 1 partition | partition 부족으로 처리 지연 |
| Spark Streaming | 로컬 단일 드라이버 | 메모리 부족, 처리 속도 미달 |
| S3 small file | trigger 30초마다 생성 | 파일 수 폭증 → Athena 쿼리 비용 증가 |
| Iceberg compaction | 수동 또는 단순 스케줄 | 컴팩션 빈도가 수집 속도를 못 따라감 |
| Glue Catalog | 단일 catalog | metadata 병목 |

### 대응 방향

**Kafka**: partition 수를 symbol 단위로 늘린다. 이후 MSK로 전환한다.

```
Local Kafka (1 partition)
→ MSK (symbol당 partition 분리)
```

**Spark**: EMR 또는 Glue Spark으로 전환해 executor를 수평 확장한다.

**Small file**: trigger interval을 늘리거나, S3 small file을 주기적으로 병합하는
배치 job을 추가한다. Iceberg compaction 주기를 데이터 유입량에 맞게 조정한다.

**Iceberg partition**: 현재 `year/month` 파티션을 `days(ingest_time)` 또는
`hours(ingest_time)`으로 세분화해 partition pruning 효율을 높인다.

**Athena → Trino/Spark SQL**: 쿼리 빈도가 높아지면 Athena 비용이 증가한다.
EMR Trino 또는 Spark SQL로 전환을 검토한다.

---

## 8. 장애·운영 시나리오

### 시나리오 1 — Spark Streaming Job OOM으로 재시작

**상황**: 새벽에 stream job이 메모리 부족으로 죽는다.

**대응**:
- `checkpointLocation`이 S3에 보존되어 있으므로 재시작 시 마지막 처리 offset부터
  이어서 읽는다. 데이터 유실 없음.
- 중복 방지는 raw가 append-only이므로 downstream MERGE의 멱등성으로 처리한다.

```bash
# checkpoint 확인
aws s3 ls s3://binance-iceberg-lake/checkpoints/raw_trades/

# 재시작
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \
  streams/stream_raw_trades.py
```

### 시나리오 2 — 집계 로직 버그로 백필 필요

**상황**: processed_trades의 집계 로직에 버그가 발견됐다. 3개월치를 재처리해야 한다.

**대응**:
- Raw Zone이 append-only로 보존되어 있으므로 raw를 기준으로 재처리가 가능하다.
- Kafka offset도 raw에 보존되어 있어 특정 구간을 재처리할 수 있다.
- 재처리 중 대시보드 일관성을 위해 serving table 갱신을 일시 중단한다.
- `expire_snapshots` 보존 기간이 백필 윈도우보다 짧으면 snapshot이 만료될 수 있으므로
  백필 기간 동안 보존 기간을 늘린다.

---

## 9. 멱등성 / 재처리 가능성 설계

### Raw Zone — 재처리 기준

Raw Zone은 append-only로 유지한다. Kafka offset, topic, partition을 함께 보존해
어느 시점의 event든 raw를 기준으로 재생성할 수 있다.

### Processed — MERGE 멱등성

MERGE INTO는 같은 키(`order_id`, `symbol+interval+open_time`)에 대해 여러 번
실행해도 결과가 동일하다. `updated_at >= target.updated_at` 조건으로 늦게 도착한
이벤트가 최신 상태를 덮어쓰지 않도록 단조성을 보장한다.

### Checkpoint — Kafka offset 보존

Spark Structured Streaming의 checkpointLocation이 S3에 유지되므로 job 재시작 시
마지막 처리 offset부터 이어서 읽는다.

### Serving — Incremental MERGE

serving table은 processed table 전체를 매번 재집계하지 않고, 변경된 구간만
incremental MERGE로 반영한다. 처리 실패 시 같은 구간을 다시 실행해도 결과가 동일하다.

---

## 디렉토리 구조

```
binance-iceberg-lakehouse/
├── README.md
├── docs/
│   ├── PRD.md
│   ├── decisions.md
│   ├── architecture.md
│   ├── simulator_design.md
│   ├── roadmap.md
│   ├── operations.md
│   ├── quicksight_metrics.md
│   └── phase1_setup_guide.md
├── collectors/
│   ├── csv_to_kafka.py
│   └── download_data.sh
├── simulators/
│   └── orders_simulator.py
├── streams/
│   ├── stream_raw_trades.py
│   ├── stream_raw_klines.py
│   └── stream_raw_orders.py
├── dags/
│   ├── lakehouse_daily_pipeline.py
│   └── iceberg_maintenance.py
├── jobs/
│   ├── common/spark_session.py
│   ├── build_processed_trades.py
│   ├── build_processed_klines.py
│   ├── build_processed_orders.py
│   ├── merge_kline_updates.py
│   ├── merge_order_status_updates.py
│   ├── build_market_hourly_summary.py
│   ├── build_order_execution_summary.py
│   ├── check_data_quality.py
│   ├── check_table_health.py
│   └── compact_tables.py
├── sql/
│   ├── 00_create_raw_tables.sql
│   ├── 01_create_namespaces.sql
│   ├── 02~09_*.sql
│   └── (health_queries/ — Phase 3에서 추가)
├── data/                 # gitignored
├── tests/
├── docker-compose.yml
└── requirements.txt
```

## 실행 안내

### Phase 0 — 데이터 확보

```bash
./collectors/download_data.sh
```

Binance Historical data (data.binance.vision, MIT 라이선스).
BTCUSDT 2024년 1~3월 trades + klines를 `data/raw/`에 다운로드한다.

### Phase 1 — Kafka + Raw Zone

상세 실행 순서는 `docs/phase1_setup_guide.md` 참조.

(이후 Phase별 실행 절차는 해당 Phase 완료 시점에 추가한다)

---

## 참고 문서

- `docs/PRD.md` — 프로젝트 정의서
- `docs/decisions.md` — 설계 결정 기록 (결정한 것 + 보류한 것 + 모르는 것)
- `docs/architecture.md` — 아키텍처
- `docs/simulator_design.md` — 주문 시뮬레이터 설계
- `docs/roadmap.md` — Phase별 작업 항목
- `docs/operations.md` — 운영 지표 / Airflow / 임계값
- `docs/quicksight_metrics.md` — 대시보드 지표 정의
- `docs/phase1_setup_guide.md` — Phase 1 실행 가이드