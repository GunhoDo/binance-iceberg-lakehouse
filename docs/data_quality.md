# 데이터 품질 이상탐지 (P4)

> PRD v2 P4 / FR-8·FR-9·FR-11. 스트리밍으로 적재된 데이터를 **코드 밖에서** 검사해
> freshness SLA 위반·gap·순서역전·NULL/0·스키마 드리프트를 `quality_events`에 적재하고
> Discord로 알린다. 순수 DE 품질 이상만 다룬다(ML/시장 이상탐지는 ROADMAP P4/P5로 이관).

## 1. 규칙 (FR-8·FR-9)

| check_name | 대상 | 판정 | severity |
|---|---|---|---|
| `FRESHNESS_SLA` | 스트림/토픽별 lag | p95 lag > SLA → 위반 (tail만 초과 시 WARN) | CRITICAL/WARN |
| `GAP` | trade_id(step=1) / klines open_time(step=interval) | 연속성 불연속 = 결측 | CRITICAL |
| `ORDER_REVERSAL` | 도착 순서 대비 id/시각 | 단조 증가 위반(뒤로 튐) | WARN |
| `NULL_OR_ZERO` | price·qty 등 필수 수치 | NULL 또는 0 | CRITICAL |
| `SCHEMA_DRIFT` | 실제 스키마 vs 기대 | 필드 결측·타입 불일치(추가 필드는 WARN) | CRITICAL/WARN |

- **FRESHNESS는 lag 지표를 재사용한다(FR-8)**: P2/P3에서 만든 `lag_ms`(produce_ts→commit_ts)를
  그대로 SLA와 비교한다. 새 계측을 추가하지 않는다.
- 규칙 본체는 `src/quality/rules.py`의 **순수 함수**다. Spark/카탈로그를 모른다.

## 2. 구조 (3계층)

```
src/quality/rules.py         순수 규칙 함수 (Spark 無) ── 단위테스트 대상, 100% 이식
   ▲ 값(집계/시퀀스)만 주고받음
src/quality/quality_scan.py  Spark 드라이버 (ScanConfig 주입) ── 카탈로그 중립
   │  read: trades / lag_samples → 규칙 적용 → 이상 레코드
   ▼  write
<catalog>.quality_events     이상 1건 = 1 row (append-only)
   │
src/quality/alert.py         Discord webhook + graceful degrade (FR-11)
```

## 3. 재사용성 — local 벤치 ↔ glue/S3 프로덕션

**탐지 로직은 카탈로그 중립이다.** `local`(hadoop/file://)과 `glue`(Glue/S3)의 차이는
SparkSession 빌더 config와 테이블 식별자 prefix에만 존재한다. read/write/SQL/메타데이터
테이블 조회는 두 환경에서 동일하다. 그래서 재사용의 조건은 "로컬이냐 S3냐"가 아니라
**하드코딩을 하지 않는 것**이다:

| 계층 | 재사용성 | 전환 방법 |
|---|---|---|
| `rules.py` | **100%** | 그대로. Spark/카탈로그 의존 없음 |
| `quality_scan.py` | 100% | `ScanConfig`(테이블 식별자·컬럼·step·SLA) + SparkSession **주입**. `local.bench.*` 하드코딩 안 함 |
| freshness 입력 | 스왑 | `freshness_source`: 벤치=`"lag"`(lag_samples 재사용), 프로덕션=`"event_ts"`(now − max(trade_time) 나이). 판정 `check_freshness`는 동일 |
| gap 대상 | 스왑 | 벤치=`trade_id`(step 1), 프로덕션 klines=`open_time`(step interval) (같은 `find_gaps`) |
| 스키마 기준 | 파라미터 | `expected_schema`를 인자로 |

프로덕션 엔트리포인트 **`src/quality/quality_scan_prod.py`**가 바로 그 얇은 래퍼다:
`get_spark()`(Glue) + `glue.binance_lakehouse.processed_trades` 설정만 만들어
`quality_scan.scan()`/`persist()`를 **그대로 호출**한다(드라이버·규칙 코드 재사용).

## 4. quality_events 스키마

`event_id · detected_at · run_id · source_table · check_name · severity · dimension ·
observed · threshold · detail`

- 벤치(`local.bench.quality_events`): `quality_scan.ensure_quality_table()`이 생성.
- 프로덕션(`glue.binance_lakehouse.quality_events`): `src/ddl/09_create_quality_events.sql`.
- 두 스키마는 일치한다(같은 드라이버가 식별자만 바꿔 적재).

## 5. 실행

```bash
# 벤치 (local hadoop) — spark-runner 컨테이너 내부에서:
#   선행: P2/P3 벤치로 local.bench.trades / lag_samples 적재됨 (docs/benchmark_lag.md)
python src/quality/quality_scan.py --run-id run010 --sla-ms 15000
# 옵션: --max-missing N (허용 gap), --webhook <url> (미설정 시 graceful, 콘솔 요약만)

# 프로덕션 (glue/S3) — AWS 자격증명 있는 환경에서:
PYTHONPATH=. spark-submit --packages <iceberg-runtime,iceberg-aws-bundle,hadoop-aws> \
    src/quality/quality_scan_prod.py --sla-ms 60000

# 적재 결과 확인
#   SELECT check_name, severity, count(*) FROM local.bench.quality_events GROUP BY 1,2;
```

- **알림 graceful degrade(FR-11)**: `--webhook` 또는 `DISCORD_WEBHOOK_URL` 미설정 시
  전송을 건너뛰고 콘솔에 요약만 출력한다. 미설정이 스캔을 멈추지 않는다.

### 실측 검증 (docker spark-runner, Iceberg local)

- **실데이터 run010(baseline)**: `FRESHNESS_SLA` CRITICAL — p95 25607ms > SLA 15000ms
  (docs/benchmark_lag.md의 baseline p95 25.61s와 일치). 그 외 규칙은 미발화(실 리플레이
  데이터가 깨끗) — 정직한 결과.
- **일부러 더럽힌 데모셋**: `GAP`(결측)·`NULL_OR_ZERO`(price null / qty zero)·
  `ORDER_REVERSAL`(cross-slice 역전) 모두 발화, 스키마 일치 시 `SCHEMA_DRIFT` **미발화
  (허위양성 없음)** 확인. → FR-8·FR-9 5종 전부 실 Spark에서 동작 확인.

## 6. 정직성 한계 (검증 전 미주장)

- **freshness dimension = config_label(벤치)**: 벤치 `lag_samples`는 토픽/심볼이 아니라 벤치
  설정 라벨로 그룹된다. 프로덕션(`quality_scan_prod.py`)은 `symbol`별 `trade_time` 나이로
  freshness를 재므로 dimension이 심볼이 된다 — 판정 함수(`check_freshness`)는 동일.
- **시퀀스 검사 수집량**: gap/순서역전은 dimension별 id를 드라이버로 collect한다(벤치 수만 건
  규모에 맞춤). 프로덕션 대용량에서는 같은 규칙을 Spark window(lag/lead)로 push-down해 후보
  행만 collect하도록 바꾸면 된다(로직 동일, 수집 경로만 교체).
- **Spark 실행은 컨테이너 필수**: 순수 규칙·알림은 로컬 unittest로 검증되나(`tests/test_quality_*`),
  드라이버의 실제 Iceberg 적재는 P0~P3와 동일하게 docker `spark-runner`에서 확인한다.
