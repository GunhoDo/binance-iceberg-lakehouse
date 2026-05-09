# Decisions

본 문서는 프로젝트 진행 중 내린 결정과 그 근거, 그리고 **결정을 보류한 항목**과
**아직 모르는 것**을 함께 기록한다. 결정한 것뿐 아니라 보류한 것을 적는 이유는,
지금 시점에서 답을 모르는 항목까지 코드에 적어 넣지 않기 위해서다.

각 결정에는 다음을 기록한다.

- 결정한 내용 (또는 보류 사유)
- 고려한 대안
- 선택 이유 / 트레이드오프
- 다시 검토할 시점

---

## D1. 도메인 — 광고 대신 거래소

### 결정

광고(impression / click / conversion) 도메인 대신 암호화폐 거래소 시장 데이터
(trades / klines / orders) 도메인을 선택했다.

### 대안

- 광고 도메인 — 스터디에서 다루던 reference 도메인.
- 결제 도메인 (PaySim 등) — 상태 변화는 풍부하지만 합성 데이터 단일 dump.
- 주식 거래 — 실데이터가 비공개라 시뮬레이터 부담이 더 큼.

### 이유

- Binance public data로 trade/kline 실데이터를 무료로 충분히 확보할 수 있다.
- 주문(order) 상태 변화가 lifecycle 동안 여러 번 발생해 Iceberg MERGE 시나리오가
  자연스럽게 정당화된다.
- trade(append-only) / kline(반복 update) / order(상태 변화)의 세 이벤트가
  성격이 명확히 다르므로 topic 분리 학습 의도가 또렷이 드러난다.

### 트레이드오프 / 정직성

- 거래소의 user-level 주문은 public data가 아니다. 따라서 `orders`는 시뮬레이터로
  생성한다. 이 부분은 실데이터인 척 가장하지 않고 명시한다 (PRD §2, §6.3,
  `docs/simulator_design.md`).

### 재검토 시점

- 데이터 소스를 다른 거래소 또는 다른 도메인으로 확장할 때.

---

## D2. Layer 명명 — Raw / Processed / Serving

### 결정

Bronze / Silver / Gold 대신 **Raw / Processed / Serving**을 사용한다.

### 이유

- 책임이 이름에서 바로 드러난다. Bronze가 무엇을 책임지는지 외부 독자는 학습이
  필요하지만 Raw는 즉시 이해된다.
- 메달리온 아키텍처와의 매핑은 README와 PRD §1에서 한 줄로 명시한다.

### 트레이드오프

- 메달리온이라는 업계 용어를 그대로 쓰지 않으므로 평가자에게 매핑을 한 번 더
  알려줘야 한다 → README와 PRD §1에서 처리.

### 재검토 시점

- 팀 단위 협업이 시작되어 외부 표준 용어로 통일이 필요할 때.

---

## D3. Kafka Topic 분리 — trades / klines / orders 3개

### 결정

이벤트 성격별로 Kafka topic을 분리한다.

### 대안

- 단일 wide topic + `event_type` 필드.

### 이유

- 이벤트별 스키마 / 볼륨 / 도착 시점 / 출처가 모두 다르다.
- 단일 topic으로 합치면 sparse union schema가 발생한다 (`price`, `quantity`,
  `open`, `high`, `low`, `close`, `order_status` 등이 이벤트 타입별로 대부분 null).
- public market data와 simulated private order data의 출처를 섞지 않는다.

### 보류 / 미정

- topic partition 수 — 초기에는 작게 시작하고 데이터 유입량을 보고 결정한다.
  PRD §16.1 참조. 현재 코드에는 적지 않는다.
- Kafka cleanup policy — MVP는 default 사용, 운영 시 재검토.

---

## D4. Processed Layer에서 trades와 klines 분리 (개정)

### 결정

processed layer에서도 `processed_trades`와 `processed_klines`를 분리한다.
초기 안에서는 `processed_market_events` 단일 테이블로 합치려 했으나 개정했다.

### 이유

- D3에서 sparse union을 피하기 위해 topic을 분리했는데, processed layer에서 다시
  합치면 같은 sparse union 문제가 silver에 그대로 옮겨진다. PRD §3 논리와 일관성을
  유지하려면 processed layer에서도 분리해야 한다.
- 두 이벤트의 분석 단위가 다르다. trade는 개별 체결 이벤트, kline은 OHLCV
  aggregate. 같은 row에 두면 의미가 흐려진다.
- 공통 KPI는 serving 단계 (`market_hourly_summary`) 에서 `symbol`과 time window
  기준으로 조합하면 충분하다.

### 트레이드오프

- table 수가 늘어난다 → DDL과 수집 job이 한 종류 더 생긴다.
- 그러나 모델링 명확성과 PRD 일관성이 더 중요하다고 판단.

### 재검토 시점

- 사용자가 trade 단위와 kline 단위를 동시에 자주 조회해 단일 테이블 자료가 필요하다고
  판단될 때 → 그 경우에도 union view로 만든다.

---

## D5. Iceberg Table Mode — COW 기본

### 결정

MVP의 모든 processed/serving table은 COW (Copy-on-Write) 로 시작한다.

### 이유

- 데이터 규모가 작은 MVP에서는 구현 단순성과 snapshot 비교의 명확성이 중요하다.
- COW는 commit마다 새 데이터 파일을 쓰므로 snapshot 단위 변경을 직관적으로 관찰할 수
  있다. 이는 본 MVP의 학습 목적(Iceberg 동작 이해)과 부합한다.

### 트레이드오프

- 같은 partition에 잦은 update가 발생하면 write 비용이 커진다.
- 그러나 MVP 단계에서는 update 빈도와 데이터 규모가 작아 문제되지 않는다.

### 재검토 시점

- `processed_klines` 또는 `processed_orders`의 update 빈도가 높아져 row-level
  update 비용이 문제가 될 때 → 해당 테이블만 MOR로 전환을 검토한다.
- Summary/serving table은 read-heavy이므로 MOR 전환 검토 대상이 아니다.

---

## D6. MERGE INTO 사용 시점 — kline / order

### 결정

다음 두 케이스에서 `MERGE INTO`를 사용한다.

- `processed_klines`: 같은 `(symbol, interval, open_time)` 키에 대해 interval이 닫히기 전까지 반복 update가 도착할 수 있다.
- `processed_orders`: 같은 `order_id`에 대해 `NEW → PARTIALLY_FILLED → FILLED` 또는 `NEW → CANCELED` 상태 전이가 발생한다.

### Staging table 사용

Phase 2 MVP에서는 MERGE source를 안정화하기 위해 staging table을 사용한다.

- `raw_klines → staging_klines → processed_klines`
- `raw_orders → staging_orders → processed_orders`

staging table은 정제된 이벤트 로그이며 append 방식으로 유지한다.  
processed table에 MERGE하기 직전에 key 단위 dedup을 수행한다.

### Kline dedup 전략

`processed_klines`는 다음 기준으로 MERGE source를 dedup한다.

- key: `(symbol, interval, open_time)`
- order: `source_offset DESC, updated_at DESC`

late event 방어는 다음 조건으로 처리한다.

```sql
source.source_offset >= target.source_offset
```

### Order dedup 전략

`processed_orders`는 다음 기준으로 MERGE source를 dedup한다.

- key: `order_id`
- order: `event_time DESC, status_rank DESC, source_offset DESC`

상태 우선순위는 다음과 같다.

| order_status | status_rank |
|---|---:|
| `NEW` | 1 |
| `PARTIALLY_FILLED` | 2 |
| `FILLED` | 3 |
| `CANCELED` | 3 |

late event 방어는 다음 조건으로 처리한다.

```sql
source.updated_at >= target.updated_at
```

향후 Airflow 도입 시에는 `batch_id` 또는 `run_id`를 추가해 실행 단위별 staging 관리를 확장한다.

---

## D7. Raw Zone — append-only, plain Parquet

### 결정

`raw_trades`, `raw_klines`, `raw_orders`는 Iceberg가 아니라 **plain Parquet**으로
S3에 적재한다. Glue External Table로 읽기만 가능하게 한다.

### 이유

- Raw는 재처리 기준이 되는 원본이다. Iceberg의 snapshot/MERGE가 필요 없다.
- Iceberg는 processed(Silver) 부터 시작한다.
- Raw에 Iceberg를 쓰면 streaming write의 small file 문제가 생겨도 Iceberg
  compaction으로 건드릴 수 없다 (Iceberg 밖의 파일이 되어버리기 때문).

### 보류 / 미정

- raw 보존 기간 — MVP에서는 무기한 보관. 운영 단계에서 비용을 보고 결정.

---

## D8. Iceberg vs 다른 테이블 포맷

### 결정

Apache Iceberg를 사용한다.

### 검토한 이유 (간단히)

- snapshot / time travel / metadata table 등의 기능을 통해 운영 가시성을 SQL로 직접
  확인할 수 있다.
- MERGE INTO 표준 SQL을 통해 상태 update를 명시적으로 표현할 수 있다.

### 모르는 것 / 학습할 것

- Iceberg MOR의 운영 디테일 (delete file 누적, position vs equality delete 차이).
  지금은 COW만 쓰므로 학습을 보류했다. MOR 검토 시점에 깊이 학습한다.
- Iceberg Glue Catalog와 local Hadoop catalog의 운영 차이.

---

## D9. 보류한 결정과 Phase 2에서 확정한 항목

| 항목 | 보류 이유 | 결정 시점 | 결정 내용 | 결정 이유 |
|---|---|---|---|---|
| Partition spec (`days(...)`, `bucket(...)` 등) | 실제 쿼리 패턴과 데이터 분포를 보고 결정해야 의미가 있다. | Phase 2 후반 | `processed_trades`: `days(trade_time)`<br>`staging_klines`: `days(open_time)`<br>`processed_klines`: `days(open_time)`<br>`staging_orders`: `days(event_time)`<br>`processed_orders`: `days(updated_at)` | Raw는 ingest time 기준으로 보관하지만, Processed/Staging layer는 event time 또는 상태 갱신 시간 기준 조회·MERGE가 중심이므로 각 도메인 시간 컬럼 기준으로 파티셔닝한다. |
| `write.target-file-size-bytes` | streaming trigger interval과 함께 결정. | Phase 2 |  |  |
| Streaming trigger interval | 데이터 유입량을 보고 결정. | Phase 1 후반 | `30 seconds` | Phase 1에서는 빠른 수집 검증이 우선이므로 짧은 trigger interval을 사용했다. small file 발생 여부는 이후 S3 파일 크기와 compaction 실험에서 확인한다. |
| Compaction 주기 | small file 발생률을 본 뒤 결정. | Phase 3 |  |  |
| `expire_snapshots` 보존 기간 | 운영 정책에 따름. | Phase 3 |  |  |
| PRD §13.5의 임계값 (`avg_file_size_mb < 64` 등) | PRD에 적힌 값은 **초기 시작 임계값**이며, 운영하며 조정한다. | Phase 3 후반 |  |  |
| Table mode (`copy-on-write`, `merge-on-read`) | table별 update 특성에 따라 다르게 결정해야 한다. | Phase 2 | `processed_trades`: COW/Append<br>`processed_klines`: MOR<br>`processed_orders`: MOR<br>Serving tables: COW | kline/order는 MERGE 기반 update가 발생할 수 있으므로 확장성을 고려해 MOR로 설계한다. trades는 append-only이며, serving은 조회 중심이므로 COW가 적합하다. |

---

## D10. Non-Goals 의식적으로 유지

### 결정

다음은 MVP에서 구현하지 않는다 (PRD §5).

- 실제 Binance user account 주문 수집
- 실제 주문 제출 / 자동매매 / 투자 전략 추천 / 수익률 최적화
- order book 전체 재구성
- Schema Registry, Kafka Connect, DLQ
- MSK 운영
- Exactly-once end-to-end 보장

### 이유

- 본 프로젝트는 거래 시스템이 아니라 Lakehouse MVP다.
- 위 항목들은 각각 별도의 프로젝트 단위 학습이 필요하다. MVP 범위를 흐리지 않는다.
- 모르는 것을 모르는 채로 코드에 적지 않는다는 원칙과도 부합한다.

---

## D11. 데이터 범위

- trades: BTCUSDT 2024년 1월 (약 5,254만 건)
- klines: BTCUSDT 2024년 1월 1분봉 (약 4만 건)

trades와, klines는 한달치만 사용한다. 한달치만으로도 파이프라인 검증에 충분하고,
3달치를 produce하면 시간이 과도하게 소요된다.

## D12. processed_trades는 MERGE 없이 Append

trades는 체결 확정 이벤트라 한 번 발생하면 수정되지 않는다.
따라서 trade_id 기준 중복 제거 후 append만 하면 충분하다.
MERGE가 필요한 것은 값이 나중에 바뀌는 klines와 orders뿐이다.

## D13. Kline `is_closed` 처리

현재 `raw_klines`는 historical kline 기반이며, `message_value`에 WebSocket close flag(`x`)가 없다.

따라서 Phase 2에서는 모든 kline을 이미 종료된 캔들로 보고 `processed_klines.is_closed = true`로 적재한다.

향후 실시간 WebSocket kline 수집 시에는 raw message에 `x` 또는 `is_closed` 필드를 포함하고, processed layer에서 이를 `BOOLEAN`으로 변환한다.

## D14. Staging table 운영 방식

Phase 2 MVP에서는 MERGE source를 안정화하기 위해 staging table을 사용한다.

- `raw_klines → staging_klines → processed_klines`
- `raw_orders → staging_orders → processed_orders`

staging table은 정제된 이벤트 로그를 append 방식으로 유지한다.  
processed table에 MERGE하기 직전, 동일한 target key가 여러 번 포함될 가능성을 고려해 `ROW_NUMBER()` 기반 dedup을 수행한다.

Dedup key는 다음과 같다.

- `staging_klines`: `(symbol, interval, open_time)`
- `staging_orders`: `order_id`

현재 테스트 데이터에서 duplicate key가 항상 관찰되는 것은 아니지만, 실시간 kline update와 주문 상태 전이 이벤트를 고려해 dedup 로직을 기본 설계로 둔다.

향후 Airflow 도입 시 `batch_id` 또는 `run_id`를 추가해 실행 단위별 staging 관리로 확장한다.

## D15. Order simulator metadata 저장 방식

`orders` simulator는 각 이벤트에 `simulated_parameters`를 포함한다.

Phase 2에서는 `simulated_parameters`를 구조화된 Map/Struct로 강제 파싱하지 않고 JSON string으로 보존한다.

이유는 `simulated_parameters` 내부에 숫자, 배열, 문자열이 함께 존재하므로 Iceberg/Athena 호환성을 고려하면 STRING 보존이 가장 단순하고 안전하기 때문이다.

향후 simulator parameter 분석이 필요해지면 별도 schema를 정의해 struct column 또는 별도 config table로 분리한다.

## D16. Processed table COW/MOR 선택 기준

Phase 2에서는 table의 update 특성에 따라 COW(Copy-on-Write)와 MOR(Merge-on-Read)를 구분한다.

| Table | Mode | 이유 |
|---|---|---|
| `processed_trades` | COW / Append | trade event는 append-only 성격이 강하고 기존 row update가 거의 없다. |
| `processed_klines` | MOR | 실시간 kline stream에서는 같은 `(symbol, interval, open_time)` 키가 interval 종료 전까지 반복 update될 수 있다. |
| `processed_orders` | MOR | 같은 `order_id`에 대해 `NEW → PARTIALLY_FILLED → FILLED` 또는 `NEW → CANCELED` 상태 전이가 발생한다. |
| Serving tables | COW | dashboard/BI 조회 중심이므로 read performance와 단순한 snapshot 비교가 중요하다. |

`processed_klines`와 `processed_orders`는 향후 데이터 증가와 update 빈도 증가를 고려해 MOR로 설계한다. MOR는 write 비용을 줄일 수 있지만, delete file 누적과 read amplification을 관리해야 한다.

따라서 Phase 3 maintenance에서는 다음 항목을 관찰하고 관리한다.

- data file count
- delete file count
- delete/data file ratio
- manifest count
- snapshot count
- `rewrite_data_files`
- `rewrite_manifests`
- snapshot expiration

## D17. Phase 3 Daily Job 멱등성 설계

Phase 3에서는 기존 Phase 2 batch job을 그대로 Airflow DAG에 연결하지 않는다.

Phase 2 job은 기능 검증과 테이블 적재 실험을 위한 구현이며, 일부 job은 raw 전체를 읽고 append하거나, execution window 없이 full aggregation을 수행한다. 이런 방식은 Airflow의 retry, re-run, backfill 환경에서 중복 적재나 불필요한 재처리 비용을 만들 수 있다.

따라서 Phase 3에서는 Airflow 실행을 전제로 한 별도 daily job을 작성한다.

Phase 3 daily job의 공통 실행 인자는 다음과 같다.

- `--start-ts`: 처리 window 시작 시각, inclusive
- `--end-ts`: 처리 window 종료 시각, exclusive
- `--run-id`: Airflow run id 또는 수동 실행 id

각 job은 `start_ts <= event_time < end_ts` 또는 해당 테이블의 도메인 시간 기준 window만 처리한다.

테이블별 멱등성 기준은 다음과 같다.

| Target table | Idempotency key | Write pattern |
|---|---|---|
| `processed_trades` | `trade_id` | `MERGE INTO`, 기존 trade는 skip |
| `staging_klines` | `(source_topic, source_partition, source_offset)` | `MERGE INTO`, 기존 Kafka offset은 skip |
| `processed_klines` | `(symbol, interval, open_time)` | `MERGE INTO`, 최신 kline 상태로 update |
| `staging_orders` | `(source_topic, source_partition, source_offset)` | `MERGE INTO`, 기존 Kafka offset은 skip |
| `processed_orders` | `order_id` | `MERGE INTO`, 최신 order 상태로 update |
| `market_hourly_summary` | `(summary_hour, symbol)` | `MERGE INTO`, 동일 summary key update |
| `order_execution_summary` | `(summary_hour, symbol)` | `MERGE INTO`, 동일 summary key update |

이 설계를 통해 같은 execution window를 여러 번 실행해도 target table에 중복 row가 누적되지 않도록 한다.

기존 `src/pipelines/` 코드는 Phase 2 구현 및 reference로 유지한다. Phase 3 Airflow-ready job은 `src/jobs/` 아래에 분리하여 작성한다.

- `src/pipelines/`: Phase 2 batch/reference jobs
- `src/jobs/daily/`: Phase 3 window-based idempotent daily jobs
- `orchestration/dags/`: Airflow DAG definitions
- `orchestration/scripts/`: Airflow 또는 수동 실행에서 사용하는 job wrapper scripts

Airflow DAG는 Spark 처리 로직을 직접 포함하지 않고, task dependency, schedule, retry, logging만 담당한다. 실제 Spark 처리 로직은 `src/jobs/daily/`에 유지한다.

## D18. Observability table은 append-only log로 유지한다

Phase 3에서는 다음 observability table을 추가한다.

- `data_quality_summary`
- `pipeline_run_summary`
- `table_health_summary`

이 table들은 최신 상태 하나만 유지하는 current-state table이 아니라, 실행별/시점별 관측 결과를 누적하는 log table이다.

따라서 update 또는 merge가 아니라 append-only 방식으로 운영한다.

| Table | Write pattern | Reason |
|---|---|---|
| `data_quality_summary` | Append only | 각 run의 품질 검사 결과를 누적한다. |
| `pipeline_run_summary` | Append only | Airflow DAG/task 실행 이력을 누적한다. |
| `table_health_summary` | Append only | Iceberg metadata 기반 table health snapshot을 시간순으로 누적한다. |

`data_quality_summary`는 row count, null count, duplicate count, check status를 저장한다.

`table_health_summary`는 Iceberg metadata table인 `files`, `manifests`, `snapshots`를 기반으로 다음 지표를 저장한다.

- data file count
- position delete file count
- equality delete file count
- delete/data file ratio
- average file size
- total size
- record count
- manifest count
- snapshot count
- last committed timestamp

향후 최신 상태만 빠르게 조회해야 한다면 `current_table_health` 같은 별도 current-state table을 만들 수 있다. 이 경우 current-state table은 MERGE 기반으로 관리한다.

---

## 모르는 것 / 학습이 더 필요한 것 (자기 인식)

이 섹션은 현 시점의 학습 격차를 의식적으로 기록한다.

- Iceberg MOR 운영 디테일 — 위 D8 참조.
- Glue Catalog 운영, IAM 권한 모델 — 클라우드 확장 시 별도 학습 필요.
- QuickSight 권한 / 데이터셋 새로고침 정책.
- Spark Structured Streaming의 checkpoint 손상 시 복구 절차 — 실제로 손상시켜 보고
  익혀야 함.
- Iceberg manifest 파일 구조 — metadata 조회는 가능하지만 내부 동작까지 깊이 알지는
  못함.

위 항목들은 코드와 문서에 단정해서 적지 않는다. Phase 5 (Maintenance) 또는
프로젝트 종료 후 학습 항목으로 별도 관리한다.
