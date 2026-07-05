# Decisions

> 이 로그는 v1~ 계속 이어진다. 아래 **과거 항목**이 참조하는 as-built 문서(`docs/architecture.md`,
> `docs/simulator_design.md` 등)와 옛 PRD 섹션 번호(§2, §6.3 등)는 **작성 당시(v2 이하) 기준**이며,
> 현재는 `prd-v2` 태그에 프리즈돼 있다(`git show prd-v2:docs/<파일>`). v3 정본 스펙은 `docs/PRD.md`.

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

## D5. Iceberg Table Mode 초기 결정 — COW 기본

### 결정

Phase 2 초기 MVP에서는 구현 단순성과 snapshot 비교의 명확성을 위해 processed/serving table을 COW(Copy-on-Write) 중심으로 시작했다.

이 결정은 이후 D16에서 table별 update 특성을 반영해 개정되었다. 현재 최종 기준은 D16을 따른다.

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
- Summary/serving table은 이후 D16에서 Airflow window 재실행과 late event 반영 가능성을 고려해 MOR 대상으로 개정했다.

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
  Phase 2~3에서 MOR table을 도입했지만, delete file 증가와 read amplification의 장기 운영 정책은 계속 학습하고 검증한다.
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
| Table mode (`copy-on-write`, `merge-on-read`) | table별 update 특성에 따라 다르게 결정해야 한다. | Phase 2 후반 ~ Phase 3 | `processed_trades`: COW/Append<br>`processed_klines`: MOR<br>`processed_orders`: MOR<br>`market_hourly_summary`: MOR<br>`order_execution_summary`: MOR<br>`Observability tables`: Append only | kline/order는 MERGE 기반 update가 발생할 수 있으므로 MOR로 설계한다. serving table도 같은 `(symbol, summary_hour)` key에 대해 late event, 재처리, Airflow window 재실행으로 반복 MERGE될 수 있으므로 MOR로 관리한다. observability table은 실행별 로그이므로 append-only로 유지한다. |

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

더 이상 Iceberg의 `MERGE INTO`를 사용하지 않고, 현재 배치에서 `trade_id`를 기준으로 중복을 제거하고, 기존 `processed_trades.trade_id`와 왼쪽 역조인(left-anti join)을 수행한 후, 새 행만 추가합니다.
이를 통해 재시도/백필의 멱등성을 유지하면서 MERGE 계획 비용, 윈도우 정렬 중복 제거 비용, 그리고 전체 대상 `COUNT(*)` 로깅을 방지할 수 있습니다.

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

Phase 3에서는 `run_id`를 job 실행 인자로 전달하고, window 기반 job에서 같은 Kafka offset 또는 business key가 재처리되어도 target table에 중복 row가 누적되지 않도록 했다. staging table 자체에 `run_id` 컬럼을 저장하는 방식은 아직 도입하지 않았으며, execution unit별 staging 추적이 필요해지면 별도 컬럼 또는 current-state table로 확장한다.

## D15. Order simulator metadata 저장 방식

`orders` simulator는 각 이벤트에 `simulated_parameters`를 포함한다.

Phase 2에서는 `simulated_parameters`를 구조화된 Map/Struct로 강제 파싱하지 않고 JSON string으로 보존한다.

이유는 `simulated_parameters` 내부에 숫자, 배열, 문자열이 함께 존재하므로 Iceberg/Athena 호환성을 고려하면 STRING 보존이 가장 단순하고 안전하기 때문이다.

향후 simulator parameter 분석이 필요해지면 별도 schema를 정의해 struct column 또는 별도 config table로 분리한다.

## D16. Processed / Serving table COW/MOR 선택 기준

Phase 2 후반부터 table의 update 특성에 따라 COW(Copy-on-Write)와 MOR(Merge-on-Read)를 구분한다.

| Table | Mode | 이유 |
|---|---|---|
| `processed_trades` | COW / Append | trade event는 append-only 성격이 강하고 기존 row update가 거의 없다. |
| `processed_klines` | MOR | 실시간 kline stream에서는 같은 `(symbol, interval, open_time)` 키가 interval 종료 전까지 반복 update될 수 있다. |
| `processed_orders` | MOR | 같은 `order_id`에 대해 `NEW → PARTIALLY_FILLED → FILLED` 또는 `NEW → CANCELED` 상태 전이가 발생한다. |
| `market_hourly_summary` | MOR | 같은 `(symbol, summary_hour)` 집계 row가 late event, 재처리, Airflow window 재실행으로 반복 MERGE될 수 있다. |
| `order_execution_summary` | MOR | 같은 `(symbol, summary_hour)` 주문 KPI row가 late order event, 재처리, incremental aggregation으로 반복 MERGE될 수 있다. |
| Observability tables | Append only | 실행별 관측 결과를 누적하는 log table이므로 기존 row를 update하지 않는다. |

`processed_klines`, `processed_orders`, `market_hourly_summary`, `order_execution_summary`는 update 또는 MERGE 가능성이 있는 table이므로 MOR로 설계한다. MOR는 write 비용을 줄일 수 있지만, delete file 누적과 read amplification을 관리해야 한다.

따라서 Phase 3 maintenance에서는 다음 항목을 관찰하고 관리한다.

- data file count
- position delete file count
- equality delete file count
- delete/data file ratio
- manifest count
- snapshot count
- `rewrite_data_files`
- `rewrite_position_delete_files`
- `rewrite_manifests`
- snapshot expiration

Observability table은 append-only log table이므로 `rewrite_position_delete_files` 대상이 아니다. 다만 small file이 누적될 경우 `rewrite_data_files` 대상에는 포함할 수 있다.

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

### Airflow / Spark 실행 구조

Phase 3에서는 Airflow 컨테이너에 Spark runtime을 직접 포함하지 않는다.

Airflow 이미지는 orchestration 전용으로 유지하고, 실제 Spark job은 `spark-runner` 컨테이너에서 실행한다.

```text
Airflow BashOperator
   ↓ docker exec
spark-runner
   ↓
run_job_with_log.sh
   ↓
run_job.sh
   ↓
src/jobs/daily/*.py
```

이 구조를 선택한 이유는 다음과 같다.

- Airflow 이미지를 Spark/JDK 의존성으로 무겁게 만들지 않는다.
- Airflow는 orchestration 책임만 가진다.
- Spark 실행 환경은 `Dockerfile.spark`에서 별도로 관리한다.
- Spark/Iceberg/Hadoop AWS dependency는 `Dockerfile.spark`에서 preloading하여 런타임 Ivy/Maven 충돌을 줄인다.
- Airflow 병렬 task 실행 시 Derby metastore 충돌을 피하기 위해 job별 `derby.system.home`을 분리한다.

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


Phase 3 구현 결과, `pipeline_run_summary`에는 Airflow task별 실행 결과가 append-only로 기록된다.

기록 대상은 다음과 같다.

- `run_id`
- `pipeline_name`
- `task_name`
- `status`
- `started_at`
- `ended_at`
- `duration_sec`
- `error_message`
- `created_at`

실패한 task와 재시도 후 성공한 task가 모두 기록된다. 이는 의도한 동작이다. `pipeline_run_summary`는 current-state table이 아니라 실행 이력 log table이므로, 같은 `run_id`와 `task_name`에 대해 여러 row가 존재할 수 있다.

## D19. Phase 3 Iceberg Maintenance 정책

### 결정

Phase 3에서는 Daily Pipeline DAG와 Iceberg Maintenance DAG를 분리한다.

Daily Pipeline DAG는 Raw → Processed → Serving → Observability 흐름을 처리한다. Maintenance DAG는 Iceberg table의 파일 수, delete file, manifest, snapshot 상태를 관리한다.

Maintenance DAG의 흐름은 다음과 같다.

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

### Table policy

| Table | Policy |
|---|---|
| `processed_trades` | COW_APPEND |
| `processed_klines` | MOR |
| `processed_orders` | MOR |
| `market_hourly_summary` | MOR |
| `order_execution_summary` | MOR |
| `data_quality_summary` | APPEND_ONLY |
| `pipeline_run_summary` | APPEND_ONLY |
| `table_health_summary` | APPEND_ONLY |

### Maintenance procedure

- 모든 maintenance 대상 table에 `rewrite_data_files`를 적용할 수 있다.
- MOR table에는 `rewrite_position_delete_files`를 적용한다.
- 모든 maintenance 대상 table에 `rewrite_manifests`를 적용할 수 있다.
- snapshot은 `expire_snapshots`로 보존 정책에 따라 정리한다.
- `remove_orphan_files`는 위험도가 있으므로 MVP에서는 실행하지 않고 skip한다.

### 이유

MOR table은 update 비용을 줄이는 대신 delete file과 manifest가 누적될 수 있다. 따라서 table health를 먼저 측정하고, maintenance를 수행한 뒤 다시 table health를 측정해야 한다.

Observability table은 append-only log table이므로 position delete rewrite 대상은 아니다. 다만 small file이 누적될 수 있으므로 data file compaction 대상에는 포함할 수 있다.

### 재검토 시점

- delete/data file ratio가 기준값을 지속적으로 초과할 때
- manifest count가 증가해 query planning 시간이 길어질 때
- snapshot 수가 증가해 metadata 관리 비용이 커질 때
- orphan file cleanup을 안전하게 검증할 수 있는 별도 실험 환경을 마련했을 때

---

## D20. Orders simulator 기간 기반 생성과 order_id namespace

### 결정

`orders` simulator는 월별 backfill 또는 dashboard KPI 검증 시, 실행 기간과 주문 ID namespace를 명시적으로 받는다.

추가한 실행 인자는 다음과 같다.

- `--start-ts`: simulated order event 생성 기간의 시작 시각
- `--end-ts`: simulated order event 생성 기간의 exclusive 종료 시각
- `--order-id-prefix`: 월/기간별 `order_id` 충돌 방지 prefix
- `--seed`: 같은 입력으로 재현 가능한 synthetic data 생성을 위한 random seed

`--start-ts`와 `--end-ts`가 모두 제공되면 각 주문의 `NEW` 이벤트 기준 시각인 `base_time_ms`를 다음 범위에서 random sampling한다.

```text
[start_ms, end_ms - MAX_LIFECYCLE_OFFSET_MS)
```

현재 `MAX_LIFECYCLE_OFFSET_MS`는 `15_000` milliseconds다. 최종 상태 이벤트(`FILLED` 또는 `CANCELED`)는 `NEW` 이후 최대 15초 안에서 생성되므로, 이 여유 구간을 빼고 sampling해 최종 이벤트가 `end_ts`를 넘지 않도록 한다.

`order_id`는 다음 형식으로 생성한다.

```text
O{order_id_prefix}{order_index:08d}
```

예를 들어 `--order-id-prefix 202402`와 `order_index = 1`이면 `O20240200000001`이 된다.

### 이유

Phase 3 window 기반 job은 `start_ts <= event_time < end_ts` 조건으로 처리한다. 따라서 2024년 2월 market data를 적재했는데 orders simulator가 현재 시각 기준 event를 만들면, 2월 window의 `order_execution_summary`에는 주문 KPI가 제대로 잡히지 않는다.

또한 `processed_orders`는 `order_id` 기준으로 최신 주문 상태를 MERGE한다. 월별로 simulator를 다시 실행할 때 `order_id`가 매번 `O00000001`부터 시작하면, 1월 주문과 2월 주문이 같은 주문의 상태 update처럼 처리될 수 있다.

따라서 market KPI와 order KPI를 같은 월/기간 축에서 비교하려면:

- market data는 해당 기간의 trades/klines를 적재한다.
- order KPI가 필요하면 orders simulator도 같은 기간으로 실행한다.
- 월별 실행에서는 `YYYYMM` 같은 `order_id_prefix`를 사용한다.

### 대안

- 기존처럼 `now_ms()`만 사용한다.
  - 빠른 smoke test는 가능하지만 월별 backfill과 dashboard KPI 검증에는 부적합하다.
- `order_id`에 UUID를 사용한다.
  - 충돌 방지는 쉽지만 월별 실행 단위가 ID만 보고 드러나지 않는다.
- `run_id` 또는 `batch_id`를 별도 필드로 추가해 MERGE key에 포함한다.
  - 더 엄밀한 실행 단위 추적이 가능하지만 `processed_orders`의 key와 downstream 설계를 함께 바꿔야 한다.

### 트레이드오프

- `--start-ts`/`--end-ts`를 주지 않으면 하위 호환을 위해 현재 시각 기반 생성이 유지된다. 단, 이 모드는 월별 KPI 검증용이 아니라 로컬 smoke test용으로 본다.
- `--order-id-prefix` 기본값은 빈 문자열이다. 기존 ID 형식과 호환되지만, 월별 재실행에서는 사용자가 prefix를 반드시 관리해야 한다.
- event time은 지정 기간 안에서 random sampling하므로 실제 주문 도착 분포를 재현하는 모델은 아니다. 현재 목적은 시장 microstructure 재현이 아니라 lakehouse MERGE/window 처리 검증이다.

### 재검토 시점

- simulator run 단위 추적이 중요해져 `run_id` 또는 `batch_id`를 schema에 추가할 때.
- 실제 private order data 또는 더 정교한 order arrival 모델을 도입할 때.
- `processed_orders`의 MERGE key를 `order_id` 단일 key가 아닌 복합 key로 바꿀 필요가 생길 때.

---

## D21. WebSocket ingestor는 CSV 리플레이와 동일한 문자열 스키마로 방출한다

### 결정

`infra/ws_to_kafka.py`(실시간 WebSocket 프로듀서)는 `trades`/`klines` 토픽 레코드의
**공통 필드를 `infra/csv_to_kafka.py`(통제 리플레이 프로듀서)와 동일한 문자열 타입**으로
방출한다. WS 전용 메타 필드(`event_type`, `exchange_event_time`, `ingest_time`, `source`,
`is_closed`)만 native 타입을 유지한다.

### 이유

두 프로듀서(리플레이=벤치 부하, WebSocket=실시간)가 **하나의 downstream**을 공유하기
때문이다(PRD v2 소스 이원화). downstream(`01_build_processed_trades.py`,
`02_build_processed_klines.py`)은 `from_json`을 **전부 StringType**으로 선언하고,
- trades: `is_buyer_maker`/`is_best_match`를 `== "True"` **문자열 비교**로 bool 변환
- klines: `open_time`/`close_time`/`number_of_trades`를 파싱 후 `cast("long")`

한다. WS의 JSON 원시 타입(int id/time, bool flag)을 그대로 흘리면:
- `is_buyer_maker`(JSON `true`)가 `== "True"`와 불일치 → **조용히 전부 false로 오염**(에러 없음)
- 정수 필드는 StringType 파싱이 버전 의존적으로 `null`이 될 위험

이는 "에러 없이 잘못된 데이터가 쌓이는" silent 정합성 결함이다. `str(True) == "True"`이므로
공통 필드를 stringify하면 CSV·WS가 **동일 downstream을 무수정으로 통과**하고, 리플레이/라이브가
구분 없이 처리된다(D12/D13 파싱 규약과 일관).

### boolean 토큰 확정 (실측)

실제 Binance 월별/일별 trades CSV의 `is_buyer_maker`/`is_best_match` 컬럼은 **대문자
`"True"`/`"False"`**를 쓴다(`data.binance.vision`의 `BTCUSDT-trades-2024-01-01` 샘플로 확인).
downstream의 `== "True"` 비교, CSV 원본, WS의 `str(bool)`(→`"True"`)이 **셋 다 일치**하므로
CSV 경로에 잠재 결함은 없고 downstream 변경도 불필요하다.

### 재검토 시점

- 라이브 kline의 미확정 캔들(`is_closed=false`) 업데이트를 downstream이 확정 캔들과 구분해야
  할 때(D13 `is_closed` 처리 재검토와 연동).
- lag 계측(P2)에서 `exchange_event_time`을 별도 스키마로 파싱하기 시작할 때.

---

## D22. (v3 / Phase G) VWAP을 서빙 컬럼으로 물질화

### 결정

`market_hourly_summary`에 시간당 **VWAP**(거래량 가중 평균가)을 저장 컬럼으로 추가한다.

```sql
vwap = Σ(quote_qty) / NULLIF(Σ(qty), 0)   -- processed_trades 를 summary_hour 로 집계
```

신규 설치는 `07_create_serving_tables.sql` baseline CREATE에 포함하고, 기존 테이블은
`10_add_vwap_market_hourly_summary.sql`의 `ALTER ... ADD COLUMN vwap ... AFTER avg_trade_price`로
마이그레이션한다(Iceberg 메타데이터 전용, 재작성 없음). `06` job은 CTE·최종 SELECT·MERGE UPDATE
세 곳에 반영한다.

### 대안

- 쿼리 시점마다 뷰/서브쿼리로 VWAP 산출 — 저장하지 않음.
- avg_trade_price(단순 평균)로 대체.

### 이유

- VWAP은 Phase X 슬리피지의 **벤치마크 기준선**이므로, 매 조회마다 52M trades를 재집계하는 대신
  시간당 1행으로 물질화하는 편이 서빙·조인 비용에서 유리하다.
- 단순 평균(avg_trade_price)은 대량 체결 가중을 반영하지 못해 벤치마크로 부적합하다.
- WS 수집(`infra/ws_to_kafka.py`)에서 aggTrade에 없는 `quote_qty`를 `price×qty`로 재구성해
  넣으므로, VWAP = Σ(price×qty)/Σ(qty) = **정통 거래량 가중 평균가**와 정확히 일치한다(근사 없음).

### 실데이터 검증 (2026-07-05, Athena)

BTCUSDT 2024-01 744시간 백필 결과: 채움 744/744, `vwap ∈ [low_price, high_price]` 위반 0건,
distinct 744(값 퇴화 없음), `corr(high−low, |vwap−avg_trade_price|)=0.46`(변동성이 클수록 가중·단순
평균 괴리가 커짐 → VWAP이 실제 시장 미세구조를 반영).

### 재검토 시점

- 멀티심볼/멀티interval로 확장해 집계 조합이 늘 때.
- VWAP 외 벤치마크(TWAP, arrival price)를 추가로 서빙할 때.

---

## D23. (v3 / Phase A) 시뮬레이터 실시장 앵커링 + 순환 방지 불변식

### 결정

합성 주문의 가격·도착을 실시장 분봉에 **앵커링**한다.

- 주문 가격 기준점 = 주문 시각이 속한 **그 분(minute)의 close**(= 의사결정 시점 가격),
  그 주변 `±PRICE_DEVIATION_RATE`(0.3%)에서 샘플링.
- 주문 도착 = 분봉 **거래량 가중** 샘플링(`random.choices(buckets, weights=volumes)`).
- 앵커 fixture는 `export_anchor_klines.py`로 `processed_klines`에서 추출(CSV: open_time_ms,close,volume),
  `--anchor-klines`로 주입. provenance(`anchor_file`/`anchor_sha256`/`anchor_mode`)를
  `simulated_parameters`에 기록.

### 순환 방지 불변식 (이 결정의 핵심)

**앵커 기준점 ≠ 벤치마크 기준점.** 앵커(가격이 붙는 기준)는 **분 close**이고, 슬리피지 벤치마크는
다운스트림에서 시간당 집계하는 **interval VWAP**이다. 둘이 같은 시계열이면(예: interval VWAP에
앵커링하고 같은 VWAP으로 슬리피지 측정) 기대 슬리피지가 **0으로 퇴화**하는 자기 파라미터 읽기가 된다.
서로 다른 시계열이어야 슬리피지 분포가 0에 붙지 않고 구간 변동성과 상관을 가진다.

### 정직한 포지셔닝

앵커링은 **필요조건이지 충분조건이 아니다.** 앵커링을 해도 주문은 무작위 산포 합성 주문이므로,
여기서 나오는 슬리피지는 "실행 스킬"이 아니라 **분포 통계**다. 정당한 용도는 (1) 파이프라인·지표의
기계적 검증, (2) 실주문/전략 연결 시 그대로 쓸 인프라 선구축 두 가지뿐이며, "전략 성과 정량화"는
실데이터 연결 이후에만 주장한다.

### 대안

- 고정 `--reference-close`(≈43000) 유지 — D20의 기존 방식. 시장과 무관한 상수라 슬리피지가
  "상수 vs 실 VWAP 거리"만 측정하게 됨(D24의 gap 아티팩트 참조).
- interval VWAP에 직접 앵커링 — 순환 발생으로 기각.

### 트레이드오프 / 구현 메모

- `--vol-linked-rates`(선택, 기본 off): 분 상대 거래량으로 부분체결/취소율 조정. 기본은 상수 유지.
- `kafka` import를 `TYPE_CHECKING` + 지연 import로 돌려 Kafka 없이 순수 생성 로직을 단위 테스트
  가능하게 함(`run()`은 순수 제너레이터 `iter_order_events`를 소비). 하위 호환: `--anchor-klines`
  미지정 시 기존 고정-reference 동작 유지.

### 실데이터 검증 (2026-07-05)

실 fixture 44,640 버킷(2024-01)에 대해: 가격 편차 위반 0(전부 분 close ±0.3%),
`corr(실거래량, 선택횟수)=0.55`(거래 몰린 분에 주문 더 도착), seed 재현성 해시 일치.

### 재검토 시점

- 실제 private order data 또는 더 정교한 arrival 모델(Poisson/Hawkes)로 교체할 때.
- 멀티심볼 fixture로 확장할 때.

---

## D24. (v3 / Phase X) 방향 분리 슬리피지와 부호 규약

### 결정

`order_execution_summary`에 벤치마크(VWAP) 대비 슬리피지를 **BUY/SELL 분리**로 추가한다.

```sql
buy_slippage_bps  = (vwap - avg_buy_fill_price)  / NULLIF(vwap,0) * 10000   -- 양수=유리(싸게 매수)
sell_slippage_bps = (avg_sell_fill_price - vwap) / NULLIF(vwap,0) * 10000   -- 양수=유리(비싸게 매도)
slippage_cost_quote = Σ filled_qty × 방향보정 가격차                         -- 양수=순유리(quote)
```

`avg_buy/sell_fill_price`는 FILLED 주문의 **filled_qty 가중** 체결가. `benchmark_vwap`은
`market_hourly_summary`를 `(summary_hour, symbol)`로 LEFT JOIN해서 가져온다. baseline DDL +
`11_add_slippage_order_execution_summary.sql`(ALTER, `AFTER total_filled_qty`로 INSERT * 위치 정합).

### 대안

- 혼합 평균 한 컬럼(방향 무시) — BUY(음수 경향)와 SELL(양수 경향)이 **부호 상쇄**되어 정보를
  잃으므로 기각(원 설계 §4.3 미결 사항을 분리로 확정).

### 부수 결정

- **X-3 DAG 엣지**: `build_market_hourly_summary >> build_order_execution_summary`. 07이 06의 vwap을
  조인하므로 순서를 우연에 맡기지 않고 명시.
- **X-4 서빙 뷰**: `execution_vs_market`은 Grafana가 Athena를 읽으므로 **Athena 뷰**로 생성하고
  DDL을 `sql/`에 기록.
- **X-5 알람**: `|슬리피지|>50bps` 임계 초과 알람(`execution` 그룹). 합성 데이터 기준 기계적 임계.

### 순환 부재 / 정직성

benchmark_vwap은 `market_hourly_summary`(시장 체결) 유래, 체결가는 `processed_orders`(주문 앵커=분
close) 유래 — 서로 다른 시계열(D23 불변식이 여기서 결실). 다만 슬리피지 자체는 여전히 합성 주문의
분포 통계다(D23 정직한 포지셔닝 계승).

### 실데이터에서 관찰한 것 — "gap 아티팩트" (중요)

현재 lakehouse의 `processed_orders` 9,000건은 **D23 앵커링 이전(구버전)** 데이터로, 고정
`reference_close=43000`으로 생성됐다(`simulated_parameters`에 `anchor_mode` 없음). 이들로 슬리피지를
계산하면(read-only 검증):

- 공식·부호 규약은 **기계적으로 정확**(수기 대조 일치, 매수 음수/매도 양수 성립, 방향분리로 상쇄 방지).
- 그러나 체결가가 ~43000에 뭉쳐 있고 실 VWAP은 42300~48000으로 흐르므로, 슬리피지가 **체결 타이밍이
  아니라 "43000 고정값 vs 실 VWAP 거리"**로 결정된다(±100~180bps, vwap이 43000에서 멀수록 증가).

이는 **D23 앵커링이 왜 필요한지를 실증**한다. misleading하므로 이 아티팩트 값은 **서빙 테이블에
기록하지 않았고**, 스키마 ALTER와 뷰만 적용했다.

### 남은 것 (X-6 백필)

의미 있는 슬리피지는 **앵커 모드 재시뮬(D23) → Kafka → 01~07 재실행**이 필요하다. 이는 전체 스택
기동을 요구하므로 별도 단계로 둔다.

### 재검토 시점

- 앵커링된 주문으로 재시뮬해 slippage_cost_quote의 절대 규모가 현실적 범위인지 확인할 때.
- 실주문/전략 연결로 "분포 통계"에서 "실행 성과"로 성격이 바뀔 때.

---

## D25. (v3) v3 실데이터 검증을 Spark 스택 대신 Athena로 수행

### 결정

Phase G/A/X의 실데이터 검증(스키마 ALTER, 백필 MERGE, fixture export, 뷰 생성, 지표 확인)을
로컬 Spark 스택(`spark-runner`) 대신 **Athena**로 수행했다.

### 이유

- `spark-runner` 컨테이너는 자격을 **EC2 InstanceProfile**에서 가져오도록 구성돼 있어(D17 실행 구조),
  로컬 노트북에서는 이미지 빌드 + 컨테이너 자격 주입 + provider 교체가 필요해 무겁다.
- Athena는 동일한 Glue 카탈로그·동일한 S3 Iceberg 테이블에 SQL을 직접 실행한다. `ALTER ... ADD COLUMNS`,
  `MERGE INTO`, `CREATE VIEW`는 Iceberg 관점에서 Spark 실행과 **동등한 쓰기**다.

### 트레이드오프 (정직성)

- ✅ **데이터 결과는 검증됨**: 지표 값이 실데이터에서 타당함(범위·비퇴화·상관·부호).
- ⚠️ **코드 경로는 미검증**: `06`/`07` job의 파이썬 코드 자체가 스택 위에서 도는 런타임(import,
  window 인자, INSERT * 정합 등)은 확인하지 못했다. Athena는 job의 **SQL 로직을 손으로 옮겨** 돌린
  것이지 job을 실행한 게 아니다. 이 런타임 검증은 EC2(정상 환경) 또는 로컬 spark-runner 기동으로 남긴다.
- Athena가 `ADD COLUMNS`를 테이블 끝에 append하는 점(위치 지정 미지원)은 named-column MERGE로
  우회했으므로 백필에는 영향 없다. 단, 실테이블 컬럼 순서가 baseline DDL과 달라질 수 있어 Spark
  `INSERT *` 경로는 정상 환경에서 별도 확인이 필요하다.

### 재검토 시점

- EC2 또는 로컬 스택을 기동해 `src/jobs/daily/*.py`를 실제 실행하는 런타임 검증을 할 때.
- k3d(Phase K)로 실행 환경을 옮겨 자격·카탈로그 접근 방식을 재구성할 때.

---

## D26. (v3) Spark s3a 자격을 InstanceProfile 고정에서 기본 체인으로 전환

### 결정

`run_job.sh`·`run_spark_sql.sh`의 `spark.hadoop.fs.s3a.aws.credentials.provider`를
`InstanceProfileCredentialsProvider`(EC2 전용)에서 **`DefaultAWSCredentialsProviderChain`**으로
바꾼다. 또한 `spark-runner` 컨테이너에 로컬 `~/.aws`를 읽기전용 마운트하고 `AWS_REGION`/`AWS_PROFILE`
env를 준다. 이로써 **로컬은 `aws configure` 프로필**, **EC2는 instance profile**로 같은 코드가 동작한다.

### 배경 (전환 전 상태)

자격 경로가 두 갈래였다.
- **Iceberg S3FileIO + GlueCatalog**(`io-impl`): AWS SDK 기본 체인(env → `~/.aws` → instance) 사용.
- **Hadoop `s3a://`**(raw parquet 01~03, 스트리밍 checkpoint): `InstanceProfileCredentialsProvider`로
  고정 → EC2에서만 동작. 게다가 `spark-runner` 컨테이너에 자격/region이 주입되지 않아 로컬 실행이 불가했다.

D25에서 "코드 경로(Spark job 런타임) 미검증"으로 남긴 원인이 이 자격 구조였다.

### 이유

- `DefaultAWSCredentialsProviderChain`은 env → system props → **프로필(`~/.aws`, `AWS_PROFILE` 존중)**
  → EC2 instance 순으로 탐색한다. 따라서 로컬(프로필)과 EC2(instance profile) **양쪽 모두** 커버하는
  strict improvement이며 EC2 회귀가 없다.
- `bootstrap_ec2.sh`가 이미 `aws configure set`으로 프로필을 만들고 `EnvironmentVariableCredentialsProvider`
  주석을 남겨둔 것에서, 프로필 기반 전환 의도가 원래 있었다.

### 트레이드오프 / 주의

- Glue는 region이 필요한데 로컬엔 EC2 메타데이터가 없으므로 `AWS_REGION`(기본 ap-northeast-2)을 env로
  명시한다. `.env`로 override 가능.
- `~/.aws` 마운트 경로가 EC2 호스트에 없으면 docker가 빈 디렉터리를 만들지만, 기본 체인이 instance
  profile로 폴백하므로 무해하다.
- 컨테이너에 자격 파일을 마운트하므로, 최소 권한 프로필 사용을 권장한다(현재 `dogun-user`).

### 재검토 시점

- k3d(Phase K)로 옮기며 IRSA/ServiceAccount 기반 자격으로 재구성할 때.
- 프로덕션에서 장기 키 대신 STS/AssumeRole 단기 자격으로 전환할 때.

---

## D27. (v3 / Phase X-6) 앵커 주문 재적재 백필 실행과 결과

### 결정 / 실행

로컬 스택(kafka + spark-runner)을 기동해 **X-6 백필**을 실제로 실행했다. 앵커 주문을
재시뮬(Phase A)해 Kafka 로 발행 → raw 적재 → 03/05/07 재처리 → order_execution_summary
슬리피지를 앵커 기준으로 재집계했다. D25 에서 미검증으로 남긴 03/05/07 **코드 경로가
실제 스파크 위에서 동작함도 함께 검증**됐다(로컬 ~/.aws 자격, D26).

### 결과 (BTCUSDT 2024-01, 실데이터)

앵커링이 D24 의 "gap 아티팩트"를 해소했다.

| | 비앵커(구버전, D24) | 앵커(X-6) |
|---|---|---|
| 체결가 | ~43000 고정에 뭉침 | VWAP 을 바짝 추종 |
| 슬리피지 평균 | 체계적 ±100~180bps | ≈ 0 (buy +0.09 / sell −0.55 bps) |
| 분포 | 한 방향 편향 | 0 중심 양방향, sd ~16bps, 범위 −44~+75 |

- **0 퇴화 아님**: |slip|<5bps 171h / 5~20bps 372h / >20bps 140h, distinct 683. 앵커≠벤치마크가
  지켜져 슬리피지가 0 한 점에 붙지 않는다.
- **변동성 상관**: buy 0.09 / sell 0.14 (양수·약함). ±0.3% 랜덤 산포 노이즈로 약하게 나오며,
  이는 "합성 무작위 주문 = 분포 통계"라는 정직한 포지셔닝(D23)과 일치한다. 과대 해석하지 않는다.

### 운영 교훈 — Kafka offset 기반 dedup 과 토픽 재생성 충돌

staging_orders 의 멱등키는 `(source_topic, source_partition, source_offset)`, 스트리밍 raw
적재는 S3 체크포인트로 소비 오프셋을 추적한다. **docker 로 Kafka 를 내렸다 올리면 토픽
오프셋이 0 부터 리셋**되는데, S3 체크포인트/기존 staging 은 옛 오프셋을 기억한다. 그 결과:
- 스트리밍 재개 시 "이미 소비함"으로 판단해 새 메시지를 건너뛴다.
- staging MERGE 가 재사용된 offset 을 중복으로 걸러 새 주문이 안 들어온다.

따라서 "처음부터" 재적재는 raw/orders + 체크포인트 + staging_orders + processed_orders 를
**모두 비우고** 다시 처리해야 한다. 적재는 스트리밍 대신 **배치 read**(startingOffsets=earliest,
endingOffsets=latest, 체크포인트 없음)로 오프셋 꼬임을 원천 차단했다.

### 재검토 시점

- 실주문/전략을 연결해 슬리피지가 "분포 통계"에서 "실행 성과"로 바뀔 때(그때 상관·부호를 재검증).
- k3d(Phase K)로 옮겨 토픽/체크포인트 수명주기가 바뀔 때 dedup 전략을 재점검.

---

## D28. (v3 / Phase K1) k3d 멀티심볼 수집 — 플레인 매니페스트 + 단일 소스 심볼

### 결정 / 실행

k3d(로컬 k8s) 위에 Kafka(KRaft StatefulSet) + ws_to_kafka 수집기 Deployment 를 올려
**3심볼(BTC/ETH/SOL) 실시간 이벤트 적재**를 달성했다(K1 완료 기준). 산출물은
`infra/k8s/`(k3d-cluster / namespace / kafka / ingestor / kustomization / deploy.sh),
`config/symbols.yaml`, `infra/Dockerfile.ingestor`.

### 핵심 선택

- **오퍼레이터 미도입**: Strimzi/Spark Operator 없이 플레인 매니페스트로 시작(ROADMAP §4.2).
  필요가 증명되면 승격. 학습·디버깅 표면을 줄이고 매니페스트로 동작을 그대로 읽는다.
- **심볼 단일 소스**: `config/symbols.yaml` 하나가 심볼·스트림·샤딩의 진실 원천. ConfigMap 으로
  주입되고, 샤드별 심볼은 Deployment command 에서 pyyaml 로 런타임 추출한다. 심볼 추가는 이 파일만
  고친다. (kustomize configMapGenerator 는 상위 디렉터리 파일 참조를 막아 deploy.sh 의 imperative
  `--from-file` 로 생성.)
- **샤딩 = 샤드당 1 WS 연결**: 바이낸스 combined-stream 1024 stream/연결 제한 대비. shard-0=BTC·ETH,
  shard-1=SOL. (symbols × streams) 증가 시 샤드 Deployment 를 늘린다.
- **격리 계승**: 브로커 수명(PVC) vs 잡 수명(Pod) 분리(v1 W1 교훈). 광고 리스너는 StatefulSet 파드
  FQDN(`kafka-0.kafka.<ns>.svc`)으로 고정.

### 대안

- Compose 유지: 단일 호스트라 멀티심볼·샤딩·자동 백필(K3) 확장이 어렵다.
- Strimzi(Kafka Operator): 운영 편의는 크나 K1 학습 목적엔 과함. 승격 후보.

### 재검토 시점

- K2(spark on k8s)에서 로컬 레지스트리로 spark 이미지 push 시 이미지 배포 방식 재점검.
- 브로커 다중화·토픽 파티션 확장이 필요해질 때 StatefulSet replicas/파티션 재설계.

---

## D29. (v3 / Phase K2) Spark on k8s — client 모드 spark-submit + 공식 이미지 + 최소권한 RBAC

### 결정 / 실행

일일 파이프라인 **01~09 잡 전부를 k3d 위 Spark-on-k8s 로 실행**했다(K2 완료 기준). 드라이버는
Job 파드로 뜨고, 드라이버가 executor 파드 2개를 직접 스케줄한다(client 모드). S3/Glue 자격은
`aws-creds` Secret 으로 주입한다. 산출물: `infra/Dockerfile.spark-k8s`, `infra/k8s/`(30-spark-rbac /
40-spark-job.template / spark_submit_k8s.sh / run_pipeline_k8s.sh / build_spark_image.sh).

검증: 2024-01-01 윈도우로 01~09 순차 실행 전부 성공. 타깃 카운트가 기존 베이스라인과 일치
(klines 44,640 / staging_orders 22,079 / processed_orders 9,000 / market_hourly_summary 744 /
order_execution_summary 744, 08 checks=5 / 09 tables=5). 정리도 확인 — executor conf-map/PVC
잔여 0, Job 은 `ttlSecondsAfterFinished` 로 자동 소멸.

### 핵심 선택

- **공식 apache/spark 이미지 베이스**(pip pyspark 아님): k8s executor 파드는 이미지 내장
  엔트리포인트(`/opt/entrypoint.sh executor`)로 뜨는데 pip pyspark 배포판엔 이게 없다.
  `apache/spark:3.5.5-scala2.12-java17-python3-ubuntu`(멀티아치 → Apple Silicon k3d 동작)에
  Iceberg/hadoop-aws jar·파이썬 의존성·저장소 `src` 를 얹는다. Compose 용 `Dockerfile.spark`(pip)
  는 그대로 두고 이미지를 분리(D26 자격 전환과 동일한 병행 원칙).
- **client 모드 (cluster 아님)**: 드라이버가 Job 파드 자신 → `kubectl logs` 로 로그·종료코드가
  바로 잡히고 Job 성공/실패에 그대로 매핑된다. executor 가 드라이버에 붙도록 `spark.driver.host`
  = 파드 IP(Downward API), bindAddress 0.0.0.0, 포트 고정(7078/7079). 별도 headless Service 불필요.
- **k3d image import (레지스트리 push 아님)**: ingestor(K1)와 동일 패턴 유지 →
  `imagePullPolicy=IfNotPresent`. D28 재검토 항목("레지스트리 push 방식")은 import 로 결론.
- **executor 는 src 코드 불필요**: 잡이 순수 Spark SQL/DataFrame(파이썬 UDF 0) → executor 는
  jar+S3자격만. 단, s3a 원천을 읽으므로 executor 에도 자격 주입 필요 →
  `spark.kubernetes.executor.secretKeyRef` 로 AWS_* 4키를 Secret 에서 env 로. 드라이버는 envFrom.
- **네임스페이스 최소권한 RBAC**: ServiceAccount `spark` + 네임스페이스 한정 Role. 클러스터 전역
  권한 없음. Spark stop() 이 executor pods/services/configmaps/**persistentvolumeclaims** 를
  `deletecollection` 으로 일괄 정리하므로 이 4종에 deletecollection 을 명시(누락 시 작업은 끝나도
  종료 정리에서 Forbidden 로그 + orphan 리소스 — 실제로 2회 반복해 좁혀 넣었다).
- **자격은 로컬 aws configure 에서 Secret 생성**: `spark_submit_k8s.sh` 가 `aws configure get` 으로
  Secret 을 idempotent 생성(git 미커밋). D26 의 DefaultAWSCredentialsProviderChain 을 k8s 로 계승.

### 대안

- **cluster 모드**: 드라이버가 별도 파드 → 로그/종료코드 수집이 번거롭고 배치 오케스트레이션엔 이점
  적음. client 모드 채택.
- **Spark Operator(SparkApplication CRD)**: 선언적이지만 K2 학습·최소 표면 목적엔 과함. 승격 후보.
- **로컬 레지스트리 push**: k3d-cluster.yaml 에 레지스트리를 두었으나 import 가 더 단순(인증 불필요).

### 재검토 시점

- K3(Airflow KubernetesExecutor, helm 필요)에서 이 Job 템플릿을 KubernetesPodOperator/Executor
  로 승격 시 spark_submit_k8s.sh 를 DAG 태스크로 대체.
- 전체 월(52.5M trades) 재적재가 필요해지면 executor instances/메모리·셔플 파티션 재설계
  (K2 검증은 실행 경로 확인이 목적 — 데이터 결과는 X-6 가 Compose 에서 이미 검증, D27).

---

## D30. (v3 / Phase K3) Airflow on k8s — helm(KubernetesExecutor) + KubernetesPodOperator + 갭 자동 백필

### 결정 / 실행

Compose 시절 orchestration(`docker exec spark-runner` BashOperator)을 **공식 apache-airflow
helm chart(KubernetesExecutor)로 k8s 에 올리고**, 각 Spark 잡을 KubernetesPodOperator 로
실행하도록 전환했다(K3). 새 갭 탐지→자동 백필 DAG 도 추가. 산출물: `infra/Dockerfile.airflow-k8s`,
`infra/k8s/airflow-values.yaml`, `infra/k8s/deploy_airflow.sh`, `orchestration/dags/lib/
spark_on_k8s.py`(KPO 팩토리), daily/maintenance DAG 전환, `orchestration/dags/gap_backfill.py`,
`src/jobs/maintenance/detect_gaps.py`.

실행 3계층: **KubernetesExecutor 워커 파드 → (KPO) Spark 드라이버 파드(client 모드) →
executor 파드**. K2 자산(binance-spark-k8s 이미지, `spark` SA, `aws-creds` Secret) 재사용.

검증(실제 스케줄러 + KubernetesExecutor): `airflow dags trigger gap_backfill` 실행 →
detect_gaps(success, 파드 3계층 라이브 확인, BTCUSDT 744/744 갭 0, XCom push) → decide(success,
XCom 읽어 분기) → no_gaps(success) / trigger_backfill(skipped, 갭 없어 정확히 미트리거) →
DagRun success. daily 파이프라인은 `build_processed_trades` 태스크가 KPO→Spark 파드로
success(동일 팩토리라 전이). DAG import 에러 0, 3개 DAG 정상 인식.

### 핵심 선택

- **chart 1.16.0 / Airflow 2.10.5 (Airflow 3 아님)**: chart 라인이 1.16.0(2.10.5) → 1.18.0
  (3.0.2)로 점프. 기존 DAG 가 Airflow 2.x 용이라 마지막 Airflow-2 chart 를 택해 재작성 리스크
  최소화. Compose 는 2.11.2 — orchestration-only 라 패치 차이 무의미.
- **KubernetesPodOperator (Spark Operator CRD 아님)**: K2 의 client-mode spark-submit 을 그대로
  태스크로 옮긴다(오퍼레이터 미도입 원칙 D28 계승). 워커→드라이버 2단 파드는 감수 — 학습·디버깅
  표면을 줄이고 K2 실행 방식을 재사용. k3d 자원 절약을 위해 K3 태스크는 executor 1개·1g 로 낮춤.
- **DAG 배포 = 이미지 베이크**: git-sync/PVC 대신 커스텀 이미지에 DAG 를 굽고 k3d import.
  재현성 우선. 트레이드오프: DAG/src 변경 시 재빌드 필요(실제로 detect_gaps.py 를 spark 이미지에
  넣느라 재빌드했다 — 새 src 잡은 spark 이미지도 재빌드해야 함).
- **자동 백필 = 데이터 기반 내부 갭**: detect_gaps 가 market_hourly_summary 의 [min,max] 범위에서
  누락된 시간(심볼별)을 left-anti 로 찾아 XCom 으로 넘기고, BranchPythonOperator 가 가장 이른 갭
  '하루'를 daily 파이프라인 logical_date 로 재실행 트리거(MVP — 연속 갭은 다음 실행이 이어감).
  벽시계 아닌 데이터 범위 기준(2024 데이터를 2026 에 돌려도 유효).
- **자격/이미지/SA 재사용**: aws-creds Secret(드라이버 envFrom, executor secretKeyRef), spark SA
  (executor 파드 생성 RBAC), binance-spark-k8s 이미지 — 모두 K2 그대로.

### 함정 / 교훈

- **Bitnami Docker Hub 카탈로그 정리(2025)**: chart 고정 `bitnami/postgresql:16.1.0-debian-11-r15`
  가 사라짐("not found"). `bitnamilegacy/postgresql` 로 이전된 동일 태그를 지정 +
  `global.security.allowInsecureImages=true`.
- **helm 훅 교착**: 마이그레이션/유저생성 잡이 기본 post-install 훅이라 `--wait` 가 scheduler
  ready 를 먼저 기다리는데 scheduler 는 마이그레이션을 기다려 데드락. `migrateDatabaseJob.
  useHelmHooks=false` + `createUserJob.useHelmHooks=false` 로 일반 매니페스트화해 해소.

### 대안 / 알려진 델타

- **Airflow 3.x(chart 1.22)**: 최신이지만 DAG breaking change 다수 → 보류(승격 후보).
- **SparkKubernetesOperator + Spark Operator**: 선언적이나 오퍼레이터 도입 필요 — 미채택.
- **pipeline_run_summary 인라인 로깅 델타**: Compose 의 run_job_with_log.sh 는 태스크마다
  pipeline_run_summary 에 INSERT 했으나, K3 는 이를 생략한다(자격/실행이 로컬 master 인 run_job.sh
  기반이라 k8s 에 부적합 + spark 이중 기동 비용). Airflow 메타데이터 DB 가 태스크 성공/시간/로그를
  네이티브로 추적 → 런 관측은 오케스트레이터가 소유. Grafana 가 이 테이블에 의존하면 별도 잡으로
  재도입 필요(재검토 항목).

### 재검토 시점

- K4(멀티심볼 E2E): detect_gaps 는 이미 심볼별 갭을 보므로 심볼 확장 시 백필 트리거를 dynamic task
  mapping 으로 다중 갭·다중 심볼 지원하도록 승격.
- 운영 승격 시: git-sync DAG 배포, 외부 Postgres/Secret, Airflow 3.x, pipeline_run_summary 재도입.

---

## D31. (v3 / Phase K4) 멀티심볼 실데이터 E2E — k3d Kafka → S3 → 심볼별 슬리피지

### 결정 / 실행

K1 이 k3d Kafka 에 쌓은 **라이브 BTC/ETH/SOL** klines/trades 를 S3 raw 로 배치 적재하고,
심볼별 파이프라인(요약/VWAP) → 심볼별 앵커 픽스처(S3) → 심볼별 주문 시뮬 → 슬리피지까지
3심볼 실데이터로 흐르게 했다. 산출물: `src/streams/batch_ingest_kafka.py`(bounded Kafka→raw
배치 적재), `infra/k8s/45-spark-generic-job.template.yaml`·`k4_run.sh`(비표준 인자 잡용 범용
Job), `infra/k8s/k4_multisymbol_e2e.sh`(오케스트레이터), `src/jobs/maintenance/
verify_multisymbol.py`·`reset_multisymbol_staging.py`, `export_anchor_klines.py`/
`orders_simulator.py` 의 S3 in/out 지원, Grafana 대시보드 2종에 `$symbol` 템플릿 변수.

검증(실데이터, 실 스케줄러): market_hourly_summary VWAP — BTC $38,706–62,982 / ETH
$1,754–1,775 / SOL $79.88–81.34. order_execution_summary 슬리피지(bps) — BTC buy +0.12
sell −0.33 / ETH buy +0.26 sell −0.77 / SOL buy −2.23 sell +3.14. 3심볼 모두 raw→
processed→summary 관통.

### 실데이터에서 드러난 함정 3가지 (정직 기록)

- **`_spark_metadata` 가 배치 append 를 가린다**: 예전 Structured Streaming parquet 싱크가
  `raw/klines`·`raw/trades` 에 `_spark_metadata/` 커밋 로그를 남겼다. `spark.read.parquet()`
  는 그 경로를 스트리밍 출력으로 인식해 **커밋 로그에 등재된 파일만** 읽고 배치로 append 한
  새 파일을 무시한다 → 신규 심볼 데이터가 안 보임(02 raw_window_rows=0). 해결: 두 경로의
  `_spark_metadata/` 삭제(삭제 후엔 전체 파일 리스팅으로 폴백, 구 데이터도 그대로 보임).
- **Kafka 오프셋 재사용 → 스테이징 dedup 충돌**: staging_klines/staging_orders 는 (topic,
  partition, offset) 로 멱등 MERGE 한다. 그러나 오프셋은 한 토픽·파티션의 현재 수명 안에서만
  유일하다. 예전 단일심볼(BTC) 데이터가 오프셋 0..N 을 이미 점유한 상태에서 k3d 의 새 멀티심볼
  메시지(역시 0..N)를 적재하면, 새 ETH/SOL 행이 옛 BTC 행과 오프셋이 겹쳐 `WHEN NOT MATCHED`
  에 걸려 드롭된다(삽입 0). 해결: E2E 시작 시 `reset_multisymbol_staging.py` 로 두 스테이징을
  비운다(raw 에서 재생성 가능한 중간 계층 + Iceberg 스냅샷 복구 가능). processed/summary 는
  비즈니스 키(klines symbol/interval/open_time, orders order_id) MERGE 라 멀티심볼이 자연 합류.
- **단일 노드 메모리 압박(7.75 GiB)**: Kafka+인제스터+Airflow+Spark(드라이버+executor×2, 2g)
  동시 상주가 노드 용량을 초과해 요약 잡(06) 드라이버가 OOMKilled. 해결: `40-spark-job.template`
  을 executor 2→1, heap 2g→1536m 로 낮추고(데이터가 작아 무해), 수동 E2E 동안 Airflow
  scheduler/webserver 를 0 으로 스케일다운해 ~2g 확보.

### 핵심 선택

- **raw 재사용 + 배치 적재(스트리밍 아님)**: `batch_ingest_kafka.py` 는 earliest→latest 로
  bounded read 후 `.mode("append")` 평문 parquet(= `_spark_metadata` 미생성). stream_raw_*.py
  와 컬럼 사영을 정확히 일치시켜 처리 계층이 그대로 파싱한다.
- **범용 Job 템플릿**: 40-spark-job 은 01~09 표준 인자(--start-ts/--end-ts/--run-id)에 고정.
  batch_ingest/anchor-export/simulator 는 인자 규약이 달라 `45-spark-generic-job` + `k4_run.sh`
  (스크립트를 base64 단일 줄로 파드에 주입 후 bash 실행)로 임의 명령을 spark 이미지에서 실행.
- **앵커 픽스처 S3 경유**: export_anchor_klines 가 processed_klines 를 심볼별 CSV 로 S3 에
  쓰고(io.StringIO→put_object), orders_simulator 가 S3 에서 받아(load) 앵커 모드로 시뮬 →
  슬리피지 벤치마크(VWAP, market_hourly_summary)와 다른 시계열 유지(순환 방지, D_slippage 계승).
- **Grafana `$symbol` 커스텀 변수**: 두 대시보드 전 패널 rawSQL 에 `symbol = '$symbol'` 주입,
  30일 창 MAX 서브쿼리도 심볼별. 서빙 계층 하나로 3심볼 전환 뷰.

### 대안 / 알려진 델타

- **스테이징 dedup 키를 비즈니스 키로 (K5 후보)**: 오프셋 재사용 함정의 근본 원인은 (topic,
  partition, offset)이 "운송 좌표"일 뿐 "레코드 정체성"이 아니라는 것 — 한 토픽·파티션의 현재
  수명 안에서만 유일해 k3d 재생성·리플레이·백필·구 데이터 오프셋 대역 중복에 깨진다. 정답은 이미
  프로젝트 안에 있다: **trades(01)는 스테이징 없이 raw→processed 직행 + `trade_id`(비즈니스 키)
  dedup 이라 이 함정을 안 겪는다.** 근본 해결은 klines 를 (symbol, interval, open_time),
  orders 를 (order_id[, status]) 로 dedup 하고 offset 은 계보 메타데이터로만 남기는 것.
  A안(staging 의 offset dedup 만 제거 → 멱등성은 processed 비즈니스 키 MERGE 전담)으로 시작해
  B안(klines/orders 도 trades 처럼 staging 제거·raw→processed 직행)으로 수렴 권장. K4 범위
  밖이라 `reset_multisymbol_staging.py` 로 우회했고, 이 리셋 스크립트는 K5 리팩터 완료 시 제거 대상.
- **executor 1개·1536m 는 k3d 단일 노드 현실 반영**: 클라우드(노드 다수)에선 원복 가능.
- **k4_multisymbol_e2e.sh 는 데모용 원샷**: 시작 시 스테이징을 리셋하므로 증분 일일 파이프라인
  (Airflow)과 목적이 다르다. 일일 파이프라인은 스테이징을 증분 유지.

---

## D32. (v3 / Phase K5) 스테이징 멱등성 키를 Kafka offset → 비즈니스 키로

### 결정 / 실행

staging_klines/staging_orders 의 dedup·MERGE 키를 **Kafka (topic, partition, offset)
에서 비즈니스 키로** 전환했다. D31 에서 드러난 오프셋 재사용 충돌의 근본 해결이다.

- **02 klines**: 배치 내 dedup 과 MERGE 를 `(symbol, interval, open_time)` 로. is_closed
  (확정봉) 우선 → offset → ingest 순으로 분봉당 최신 1건 유지, MATCHED→UPDATE / NOT
  MATCHED→INSERT (UPSERT).
- **03 orders**: `(order_id, order_status, event_time)` 로. 주문은 라이프사이클 이벤트
  스트림(NEW/PARTIALLY_FILLED/FILLED/CANCELED)이라 이벤트 단위 키를 쓴다 — 05 가 한 주문의
  전체 이벤트 이력으로 상태·타임스탬프를 재구성하므로 이벤트를 잃으면 안 된다.
- Kafka offset 은 정체성에서 은퇴, 계보/타이브레이크(같은 배치·클러스터 수명 내 최신 tick
  판별)로만 남는다.
- K4 의 우회책 `reset_multisymbol_staging.py` + 오케스트레이터 step 0 제거(불필요).

### 왜 offset 이 정체성으로 부적합했나

`(topic, partition, offset)`은 **운송 좌표**지 **레코드 정체성**이 아니다. 한 토픽·파티션의
*현재 수명* 안에서만 유일 → k3d 재생성, 리플레이, 백필, 구 단일심볼 데이터가 같은 오프셋
대역을 이미 점유한 상황에서 새 멀티심볼 메시지가 같은 오프셋을 재사용하면 `WHEN NOT MATCHED`
에 걸려 **다른 심볼 레코드가 드롭**됐다(D31 의 ETH/SOL 소실). 비즈니스 키는 클러스터·오프셋
수명과 무관하게 안정적이라 이 버그 종류 자체가 사라진다.

정답 패턴은 이미 프로젝트 안에 있었다: **trades(01)는 스테이징 없이 raw→processed 직행 +
`trade_id`(비즈니스 키) dedup 이라 이 함정을 처음부터 안 겪었다.** K5 는 klines/orders 를 이
검증된 패턴에 맞춘 것.

### 핵심 선택

- **A안(스테이징 유지, 키만 교체) 채택**: 스테이징 계층·04/05/06/07 구조를 유지하고 02/03 의
  dedup·MERGE 키만 비즈니스 키로. 변경 표면 최소·다운스트림 무영향(04 는 여전히 분봉당 최신,
  05 는 전체 이벤트 이력 집계 — 오히려 오프셋 중복 이벤트가 없어 더 깔끔).
- **UPSERT (MATCHED→UPDATE SET \*)**: 기존 `WHEN NOT MATCHED INSERT` 만으로는 재적재 시 최신
  버전 반영이 안 됨. 비즈니스 키가 같으면 최신으로 갱신.
- **orders 키에 event_time 포함**: order_status 만으론 다중 PARTIALLY_FILLED 가 충돌. 셋 다
  non-null 보장(파싱 필터) + 시뮬레이터 결정적 출력이라 재적재 멱등.

### 검증

정적 정책 테스트 재작성(`test_staging_jobs_are_idempotent_by_business_key_not_kafka_offset`):
02=(symbol,interval,open_time), 03=(order_id,order_status,event_time), UPSERT, 오프셋 정체성
키 부재 단언. 로컬 23 tests OK. 통합 증명(k3d): 스테이징을 **BTCUSDT 만 남기고** 삭제(구
데이터가 오프셋 0..N 점유 = 버그 유발 조건 재현) → 새 02/03 실행 → 스테이징에 ETH/SOL 재등장
= 오프셋 충돌 없이 비즈니스 키로 삽입됨을 확인.

### 대안 / 알려진 델타 (B안 = 선택적 단순화, 대가 있음)

- **B안(스테이징 제거, raw→processed 직행)은 "수렴 권장"이 아니라 트레이드오프 있는 선택지다.**
  klines/orders 도 trades 처럼 스테이징을 없애면 잡·테이블이 줄어 표면상 단순해 보이지만,
  스테이징은 실제 역할이 있다: (1) raw JSON→타입 컬럼 파싱·정규화(메달리온 silver), (2) 특히
  orders 는 staging_orders = **이벤트 로그**(전체 라이프사이클) ≠ processed_orders =
  **현재 상태**라 계층 분리가 정보를 보존한다, (3) raw 재파싱 없이 재처리 가능. trades 에
  스테이징이 없는 건 "스테이징이 나빠서"가 아니라 trades 가 단순 append-only 라 필요없어서다.
  따라서 K5 는 A안(스테이징 유지·키만 교체)으로 버그를 이미 제거했고, **B안은 기본 계획이
  아니다** — 구체적 운영 단순화 압력이 생길 때만, 위 세 역할 상실을 감수하고 재검토할 선택지.
- offset 은 processed 계층(04/05)에서도 타이브레이크로 남아 있다(정체성 아님) — 유지.

---

## 모르는 것 / 학습이 더 필요한 것 (자기 인식)

이 섹션은 현 시점의 학습 격차를 의식적으로 기록한다.

- Iceberg MOR 운영 디테일 — 위 D8 참조.
- Glue Catalog 운영, IAM 권한 모델 — 클라우드 확장 시 별도 학습 필요.
- Grafana 권한 / datasource 운영 정책.
- Spark Structured Streaming의 checkpoint 손상 시 복구 절차 — 실제로 손상시켜 보고
  익혀야 함.
- Iceberg manifest 파일 구조 — metadata 조회는 가능하지만 내부 동작까지 깊이 알지는
  못함.

위 항목들은 코드와 문서에 단정해서 적지 않는다. Phase 5 (Maintenance) 또는
프로젝트 종료 후 학습 항목으로 별도 관리한다.
