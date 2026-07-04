# ROADMAP — 확정 실행 계획 (Gold 고도화 → 앵커링 → 슬리피지 → k3d 멀티심볼)

> **상태: 확정 (2026-07-04).** 현재 정본 스펙은 `docs/PRD.md`(v3). v2(P0~P5)는 `prd-v2` 태그에 프리즈.
> 이 문서는 v2 완료 이후의 **확정 실행 트랙(Phase G/A/X/K)** 과 **보류 항목(ML·Flink)** 을 정의한다.
> Gold 서빙 개선의 상세 설계는 `docs/gold_serving_improvement_plan.md` 참조.
>
> 확정 원칙:
> 1. **실데이터 지표 먼저** — 위험 0인 VWAP(실 체결 기반)부터.
> 2. **정직성** — 슬리피지는 시뮬레이터를 실시장에 앵커링한 **뒤에만** 서빙한다(§의존성 참조).
> 3. **ML은 보류** — 파이프라인·운영화(k3d)가 끝나기 전에는 착수하지 않는다(§7).

---

## 0. 우선순위 요약

| 순위 | Phase | 내용 | 의존성 | 규모 |
|---|---|---|---|---|
| 1 | **G** | Gold 1단계 — `market_hourly_summary`에 VWAP 벤치마크 | 없음 (실데이터만) | ~1일 |
| 2 | **A** | 시뮬레이터 실시장 앵커링 (가격·도착률·체결률) | 없음 | 2~3일 |
| 3 | **X** | Gold 2단계 — 슬리피지 지표 + 서빙 뷰 + 알람 | **G + A 완료 필수** | 2~3일 |
| 4 | **K** | k3d 멀티심볼 운영화 (BTC 단일 → 다심볼) | G/A/X와 독립, 마지막 배치 | 1~2주 |
| 보류 | — | ML 이상탐지·MLOps 루프 (§7), Flink 비교 벤치 (§8.2) | K 이후 재평가 | — |
| 백로그 | — | DuckDB 로컬 리포팅 (§8.1) — 독립 quick-win, 틈날 때 | 없음 | ~1일 |

**의존성이 순서를 강제하는 이유**: 현 `orders_simulator.py`는 `--reference-close` 단일 고정값 ±0.3%로 주문가를 뿌린다. 이 상태에서 슬리피지(체결가 vs 실시장 VWAP)를 계산하면 "고정 기준값과 실시장의 괴리"를 측정할 뿐인 무의미한 숫자가 나온다. 따라서 **VWAP(G) → 앵커링(A) → 슬리피지(X)** 순서는 선택이 아니라 강제다.

---

## 1. Phase G — Gold 1단계: VWAP 벤치마크

> 원 설계: `gold_serving_improvement_plan.md` §3 (I1). 실 체결 데이터(`processed_trades`)만 사용 — 시뮬레이터와 무관, 위험 0.

### 1.1 변경 대상

| # | 작업 | 파일/대상 |
|---|---|---|
| G-1 | DDL: `ALTER TABLE market_hourly_summary ADD COLUMN vwap DECIMAL(20,8)` | Athena/Spark SQL 1회 실행, DDL을 `sql/` 아래 기록 |
| G-2 | `trade_hourly` CTE에 `SUM(quote_qty)/NULLIF(SUM(qty),0) AS vwap` 추가 | `src/jobs/daily/06_build_market_hourly_summary_window.py` (trade_hourly CTE, 최종 SELECT, MERGE UPDATE 절 3곳) |
| G-3 | 백필: 기존 기간 전체를 윈도우 잡으로 재실행 (MERGE 멱등이라 안전) | Airflow 수동 트리거 or 로컬 spark-runner |
| G-4 | Grafana `market_hourly_summary` 패널에 `avg_trade_price` vs `vwap` 오버레이 | 기존 대시보드 수정 |

### 1.2 완료 기준
- 임의 1시간 구간을 수기 계산(`Σ quote_qty / Σ qty`)한 값과 일치.
- 거래 공백 시간대에 `vwap IS NULL` (NULLIF 방어 확인).
- `avg_trade_price`와 `vwap`의 괴리가 거래 쏠림 시간대에서 관측됨 (sanity check).
- 같은 윈도우 재실행 시 값 불변 (멱등성).

---

## 2. Phase A — 시뮬레이터 실시장 앵커링

> 원 후보: 구 로드맵 §13.1. 합성 주문을 실시장 klines에 "붙들어매"되, **seed 고정 + 고정 입력 파일**로 결정성(재현성)을 유지한다. 합성을 실데이터인 척하지 않는다(정직성) — `simulated_parameters`에 앵커 출처를 기록.

### 2.1 설계

**입력 픽스처**: `(open_time_ms, close, volume)` minute-bucket CSV. 신규 export 스크립트 `src/simulators/export_anchor_klines.py`가 `processed_klines`에서 (symbol, 기간)을 추출해 생성한다 (레이크하우스 데이터 재사용 — REST rate limit 무관). 픽스처는 `data/fixtures/` 아래 두고 sha256을 기록한다.

**`orders_simulator.py` 변경** (기존 `--reference-close` 경로는 하위 호환으로 유지):

| # | 작업 | 상세 |
|---|---|---|
| A-1 | `--anchor-klines <csv>` 옵션 + `load_anchor_frame()` | minute 버킷 리스트 + 누적 volume 가중치 로드. 파일 sha256 계산 |
| A-2 | **가격 앵커링**: `sample_order_price()`가 주문 시각 T가 속한 minute의 실 close 주변 ±0.3%에서 샘플링 | 기존 `PRICE_DEVIATION_RATE` 유지, 기준값만 시각별 실 close로 교체 |
| A-3 | **도착률 앵커링**: `sample_base_time_ms()`를 균등 샘플 → **minute volume 비례 가중 샘플**로 교체 (`random.choices(buckets, weights=volumes)` + 버킷 내 균등) | 총 주문 수(`--num-orders`)는 불변, 분포만 실 거래량을 따름 |
| A-4 | (선택, 기본 off) `--vol-linked-rates`: minute 실현변동성 z-score에 취소율 연동 (예: `cancel_rate = clip(0.25·(1+0.5z), 0.05, 0.6)`) | 롤아웃 중 채택 여부 결정 — 과도한 가정이면 버린다 |
| A-5 | `simulated_parameters`에 `anchor_file`, `anchor_sha256`, `anchor_mode` 기록 | 정직성: "앵커링된 합성"임을 데이터 자체가 증언 |
| A-6 | 테스트: 같은 seed+픽스처 → 이벤트 시퀀스 해시 동일 / 주문가가 해당 minute close ±0.3% 이내 / 시간대별 주문 수와 실 volume의 상관 > 0.7 | `tests/test_orders_simulator_anchor.py` 신규 |

### 2.2 경계 (원 후보의 원칙 유지)
- 여전히 **합성 데이터**다 — private 주문의 대체이지 실주문이 아니다. ML 학습용이 아니라 파이프라인(MOR/MERGE/실행 KPI) 검증 **픽스처**.
- 모델로 데이터를 **생성**하는 방향(GAN 등)은 신뢰도·재현성·DE 정체성을 깎으므로 비채택.
- **앵커 기준점 ≠ 벤치마크 기준점 (순환 방지 — 불변 조건)**: 앵커는 주문 시각 T의 **minute close**(의사결정 시점 가격), 슬리피지 벤치마크는 **구간 VWAP** — 반드시 서로 다른 시계열이어야 한다. 구간 VWAP에 앵커링하고 같은 VWAP으로 슬리피지를 재면 기대 슬리피지 ≈ 0, 남는 건 시뮬 산포 노이즈뿐인 **자기 파라미터 되읽기 순환**이 된다. 앵커 소스가 벤치마크 소스와 같아지는 순간 Phase X의 슬리피지는 무효. (상세: `gold_serving_improvement_plan.md` §4.1.1)

### 2.3 완료 기준
- 주문가 시계열이 실 close 곡선을 따라감 (오버레이 차트로 육안 + A-6 테스트 통과).
- 같은 seed로 재실행 시 바이트 동일 재현.
- 앵커링된 주문으로 기존 04/05/07 잡 체인이 무변경 통과.
- **앵커 소스 검증**: 픽스처가 minute close(구간 VWAP 아님)에서 왔음을 확인 — §2.2 순환 방지 조건.

---

## 3. Phase X — Gold 2단계: 슬리피지 + 서빙 뷰 + 알람

> 원 설계: `gold_serving_improvement_plan.md` §4~5 (I2, I3), 롤아웃 R2~R4. **G + A 완료가 전제.**
>
> **정직한 포지셔닝 (앵커링은 필요조건이지 충분조건이 아니다)**: 앵커링을 해도 주문은 무작위 산포 합성 주문이므로, 이 단계의 슬리피지는 "실행 스킬"이 아니라 **분포 통계**다. 정당한 용도는 (1) 파이프라인·지표의 기계적 검증, (2) 실주문/전략이 연결될 때 그대로 쓸 인프라 선구축 — 두 가지뿐이며, "전략 성과 정량화"는 실데이터 연결 이후에만 주장한다. (상세: `gold_serving_improvement_plan.md` §4.1.2)

### 3.1 방향 분리 결정 (원 설계 §4.3의 미결 사항 — 확정)

시간 버킷 안에 BUY/SELL이 섞이므로 **방향 분리 컬럼**으로 확정한다. 혼합 평균 한 컬럼은 부호가 상쇄되어 정보를 잃는다.

```sql
ALTER TABLE glue.binance_lakehouse.order_execution_summary
    ADD COLUMNS (
        benchmark_vwap        DECIMAL(20, 8),   -- market_hourly_summary.vwap 조인
        avg_buy_fill_price    DECIMAL(20, 8),
        avg_sell_fill_price   DECIMAL(20, 8),
        buy_slippage_bps      DOUBLE,           -- (vwap - buy_fill)/vwap × 10000, 양수=유리
        sell_slippage_bps     DOUBLE,           -- (sell_fill - vwap)/vwap × 10000, 양수=유리
        slippage_cost_quote   DECIMAL(30, 8)    -- 방향 보정 금액 환산 (Σ filled_qty × 가격차)
    );
```

### 3.2 변경 대상

| # | 작업 | 파일/대상 |
|---|---|---|
| X-1 | `order_hourly` CTE에 side별 체결가중 평균 체결가(FILLED만) 추가 | `src/jobs/daily/07_build_order_execution_summary_window.py` |
| X-2 | `market_hourly_summary`를 `(summary_hour, symbol)`로 LEFT JOIN → `benchmark_vwap`, side별 slippage_bps, `slippage_cost_quote` 산출 | 동일 파일, 최종 SELECT + MERGE UPDATE |
| X-3 | DAG 의존성 엣지 추가: `build_market_hourly_summary >> build_order_execution_summary` (현재 06→07 순서가 우연히 지켜질 뿐 엣지가 없음) | `orchestration/dags/daily_lakehouse_pipeline.py` |
| X-4 | 서빙 뷰 `execution_vs_market` — Grafana가 Athena를 읽으므로 **Athena 뷰**로 생성, DDL은 `sql/`에 기록 | 원 설계 §5의 SELECT 그대로 + side별 슬리피지 컬럼 |
| X-5 | Grafana: 슬리피지 추세(bps)·비용(quote) 패널 + **임계 초과 알람** (기존 alerting provisioning에 규칙 추가) | `dashboard/grafana/provisioning/alerting/` |
| X-6 | 백필: 앵커링된 주문 재시뮬레이션 → 04/05/07 재실행 (멱등 MERGE) | — |

### 3.3 완료 기준 (원 설계 §7 검증 관점 계승)
- 임의 시간대 수기 계산과 slippage_bps 일치, 양수=유리 부호 규약이 BUY/SELL 모두 성립.
- **순환 부재 검증**: 슬리피지 분포가 0 주변으로 퇴화하지 않고 구간 변동성(high−low)과 상관을 가짐 — anchor≠benchmark가 실제로 지켜졌다는 증거.
- 벤치마크 결측(매칭 market row 없음) 시 NULL 전파 확인.
- 같은 윈도우 MERGE 재실행 시 값 재현 (멱등성).
- 대시보드에서 변동성(high−low)·유동성(quote_volume) 큰 시간대와 슬리피지의 cross-reference 가능.

---

## 4. Phase K — k3d 멀티심볼 운영화

> 목표: 단일 BTCUSDT·단일 Compose에서 벗어나 **다심볼(BTC/ETH/SOL 등) 파이프라인을 k3d(로컬 k8s) 위에서 운영**. 구 로드맵의 W2(수동 백필)·W3(k8s 미적용)를 여기서 해소한다.
>
> 유리한 전제: `ws_to_kafka.py`는 이미 `--symbols` 복수 구독 지원, gold 테이블은 이미 `(summary_hour, symbol)` 키 — 데이터 모델은 멀티심볼 준비 완료. 남은 것은 시뮬레이터 픽스처(심볼별)와 실행 환경이다.

### 4.1 서브 단계

| 단계 | 내용 | 핵심 산출물 / 완료 기준 |
|---|---|---|
| **K1** 클러스터 + 수집 | k3d 클러스터(`infra/k8s/k3d-cluster.yaml`, 로컬 레지스트리) · Kafka StatefulSet+PVC(브로커 수명 분리 원칙 계승) · ingestor Deployment — `config/symbols.yaml`을 단일 소스로 ConfigMap 주입, 심볼셋 샤드 단위 Deployment(예: shard-0=BTC·ETH, shard-1=SOL·…) | 3심볼 이상 실시간 이벤트가 k3d 내 Kafka 토픽에 적재 |
| **K2** Spark on k8s | spark-runner 이미지를 k3d 레지스트리에 push · `spark-submit --master k8s://` 배치 잡 실행 (ServiceAccount/RBAC, S3 자격은 Secret) · 체크포인트 S3 격리 유지 | 01~09 윈도우 잡이 k8s Pod로 완주, 결과가 Compose 실행과 행수/합계 동일 |
| **K3** Airflow KubernetesExecutor | Airflow 공식 helm chart(KubernetesExecutor) · 기존 DAG의 `docker exec` BashOperator → k8s Pod 기반 태스크로 전환 · gap 감지 → 자동 백필 DAG (W2 정식 해소) | 태스크마다 Pod 생성·종료, 자동 백필 1회 시연 |
| **K4** 멀티심볼 E2E | 심볼별 앵커 픽스처 생성(Phase A 스크립트 재사용) · 심볼별 시뮬레이터 k8s Job · Grafana 대시보드 symbol 템플릿 변수화 | 3심볼의 VWAP·슬리피지가 한 대시보드에서 심볼 선택으로 조회 |

### 4.2 원칙
- **비용/복잡도 가드**: k3d는 로컬 무료. 오퍼레이터(Strimzi, Spark Operator)는 1차에서 도입하지 않는다 — 플레인 매니페스트 + spark-submit k8s 모드로 시작, 필요가 증명되면 승격.
- **격리 원칙 계승**: 브로커 수명(PVC) vs 체크포인트(S3) vs 잡 수명(Pod TTL) 분리 — v1 W1 교훈의 일반화.
- **롤백 안전**: Compose 스택은 K2 검증(행수/합계 동일) 전까지 유지. k3d 매니페스트는 `infra/k8s/`에 격리.

---

## 5. 성공 지표 (확정 트랙)

- **G/X**: VWAP·슬리피지 수기 검증 일치, 재실행 멱등성 100%, 슬리피지 알람 동작 1회 이상 시연.
- **A**: 같은 seed 바이트 동일 재현, 주문 분포와 실 volume 상관 > 0.7, 앵커 출처가 데이터에 자기 기술.
- **K**: k8s에서 전 파이프라인 완주(Compose와 결과 동일), 자동 백필로 gap 자동 복구 1회, 3심볼 E2E 대시보드.

---

## 6. 리스크 & 대응

| 리스크 | 대응 |
|---|---|
| 앵커링을 "실데이터"로 오인·과장 | `simulated_parameters`에 앵커 출처 자기 기술 + 문서·데모에서 항상 "앵커링된 합성" 명시 |
| **순환논리**: 구간 VWAP에 앵커링 후 같은 VWAP으로 슬리피지 측정 → 기대 슬리피지 ≈ 0의 자기 되읽기 | 앵커=minute close, 벤치마크=구간 VWAP으로 시계열 분리 강제(§2.2). A·X 완료 기준에 순환 부재 검증 포함 |
| 합성 주문 슬리피지를 "전략 성과"로 과장 → "합성 주문으로 무슨 실행 성과냐" 반박에 노출 | 분포 통계로만 포지셔닝(§3 caveat). 용도를 기계적 검증 + 인프라 선구축으로 한정 명시 |
| 슬리피지를 앵커링 전에 서빙 | 순서 강제(§0 의존성). X는 A 완료 기준 통과 전 착수 금지 |
| k3d 범위 폭주 (오퍼레이터·멀티브로커·GKE) | K1~K4 각자 완료 기준으로 끊는다. 오퍼레이터 비도입 원칙(§4.2). 클라우드 k8s는 로드맵 밖 |
| 멀티심볼로 데이터량 증가 → 로컬 자원 한계 | 심볼 3~5개 상한, 시뮬레이터 주문 수 심볼당 조정, 필요 시 심볼 샤드만 축소 |
| 방향 분리로 컬럼 증가 → 서빙 복잡화 | 뷰(`execution_vs_market`)가 소비 표준 — 테이블 직접 조회는 내부용 |

---

## 7. 보류 — ML 이상탐지 · MLOps (Phase K 이후 재평가)

> **의도적 보류.** 구 로드맵의 G4~G6·FR-8~FR-12·§6(모델 상세)에 해당. "ML은 파이프라인의 다운스트림 소비자"라는 원칙에 따라, 소비할 파이프라인(G/A/X/K)이 완성되기 전에는 착수하지 않는다. 재개 시 아래 요약에서 복원한다.

- **데이터 품질 이상탐지(순수 DE)**: 결측 캔들 gap·프레시니스 SLA·스키마 드리프트·순서역전 감지 → `anomaly(kind=quality)` + 자동 백필 트리거 연동. — 일부는 v2 P4(품질 스캔)로 이미 구현됨. 잔여분은 K3 자동 백필과 연동할 때 재검토.
- **시장 이상탐지(ML)**: 단순→복잡 단계 도입 (① EWMA z-score/STL 잔차 통계 베이스라인 → ② IsolationForest/One-Class SVM → ③ 선택적 시퀀스 모델). feature 테이블(롤링 통계: 로그수익률·실현변동성·거래량 z-score)을 학습/추론 공용으로 두어 train/serve skew 방지.
- **MLOps 루프**: k8s 학습 Job → 레지스트리(MLflow 또는 S3+메타 JSON: 데이터범위·하이퍼파라미터·메트릭·git sha) → Airflow 재학습 DAG → 드리프트(PSI/KS) 트리거 → champion/challenger 승격.
- **평가 원칙**: 정답 라벨 부재 → 합성 주입 이상(스파이크/플래시크래시)으로 recall 측정. **검증 전 성능 미주장.**
- **영구 Non-Goal**: 실매매/주문 실행, 가격 예측 헤드라인(백테스트+caveat 없이는 언급 금지), SOTA 모델 추구.

---

## 8. 백로그 — 독립 후보 (트랙 밖, 필요 시 채택)

### 8.1 DuckDB — 경량 로컬 리포팅 (quick-win, 위험 0)
`lag_report.py`의 Spark JVM 콜드스타트·aarch64 `percentile_approx` SIGSEGV 땜빵을 제거하고, append-only 벤치 테이블(`lag_samples`·`quality_events`·bench `trades`)을 DuckDB 직독 → 인프로세스 ms 리포트 + 정확 백분위(`quantile_cont`). **읽기/리포팅 전용 보조** — 스트리밍 적재·MOR position-delete 조회는 대체하지 않으며, 측정값 자체를 바꾸지 않는다. 확정 트랙과 독립이라 틈날 때 ~1일로 처리 가능.

### 8.2 Flink — lag 벤치 비교 팔 (보류)
P3 결론("tail latency는 트리거 정책이 지배")의 대안 검증: 동일 통제 리플레이 부하에 Flink 소비자를 붙여 p50/p95/p99 재비교. 고비용(신 스택)·고임팩트. 측정 정의(`commit_ts − produce_ts`)·부하 동일 통제가 전제. **ML과 함께 K 이후 재평가** — 8.1(리포팅 개선)과 서사를 섞지 말 것.

---

## 9. 다음 액션

1. **Phase G 착수** — `vwap` DDL + 06 잡 수정 + 백필 (~1일).
2. Phase A — 앵커 픽스처 export 스크립트 → 시뮬레이터 앵커링 → 재현성 테스트.
3. Phase X — 방향 분리 슬리피지 + Athena 뷰 + Grafana 알람.
4. Phase K — k3d K1부터 순차, 각 서브 단계 완료 기준으로 데모 기록.
5. 각 Phase 완료 시 README에 근거(수기 검증·재현 해시·k8s 완주 로그)를 남긴다.
