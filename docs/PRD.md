# PRD — Binance Lakehouse v3: Gold 실행성과(슬리피지) 서빙 + k3d 멀티심볼 운영화

> 대상 직무: 데이터 엔지니어
> 버전 계보: v1(`prd-v1` 태그) → v2(`prd-v2` 태그, **P0~P5 delivered**) → **현재 PRD = v3**.
> v2 성과(스트리밍 end-to-end lag **p95 25.6s → 12.9s, 약 50% 단축** + 품질 이상탐지 + 관측)의
> 실측 근거·문서는 `prd-v2` 태그에 프리즈돼 있다. 이 PRD는 그 위에 **Gold 서빙 레이어를 고도화**하고
> 단일 심볼을 **k3d 멀티심볼로 운영화**한다.
> 설계/실행 상세: `docs/ROADMAP.md`(Phase G/A/X/K), `docs/gold_serving_improvement_plan.md`.

---

## 0. 스코프 결정

v2는 "스트리밍 lag 최적화 벤치마크" 한 방에 집중해 완결했다(P0~P5). v3는 완결된 파이프라인 **위에** 두 가지를 얹는다.

1. **Gold 실행성과 지표** — 실 체결 VWAP 벤치마크 → 시뮬레이터 실시장 앵커링 → 방향 분리 슬리피지 → 서빙 뷰/알람.
2. **멀티심팀 운영화** — 단일 BTCUSDT·docker-compose에서 **다심볼·k3d(로컬 k8s)** 로 확장.

**ML 이상탐지·MLOps는 의도적으로 보류한다**(`docs/ROADMAP.md` §7). "ML은 파이프라인의 다운스트림 소비자"라는 원칙에 따라, 소비할 파이프라인(Gold+멀티심볼)이 완성되기 전에는 착수하지 않는다.

---

## 1. 배경 & 문제 정의

### 1.1 현재 상태 (v2 delivered)
- Kafka(KRaft) → Spark Structured Streaming → Iceberg(Glue/S3) → Athena/Grafana, docker-compose 단일 머신.
- 실시간 WebSocket 수집(P0), 통제 리플레이 lag 벤치마크(P1~P3), 데이터 품질 이상탐지(P4), 관측/알림(P5).
- Gold 서빙: `market_hourly_summary`(시황), `order_execution_summary`(주문 실행). **단, 두 테이블이 서로를 참조하지 않아** "우리 주문이 그 시점 시장가 대비 얼마나 유불리하게 체결됐나"에 답하지 못한다.

### 1.2 v3가 해결하는 것
| # | v2의 한계 | v3에서의 해결 |
|---|---|---|
| L1 | 시황 요약에 **거래량 가중 기준가(VWAP)** 부재 — 단순 평균가만 존재 | `market_hourly_summary`에 VWAP 컬럼 (실 체결 기반, 위험 0) |
| L2 | 주문 시뮬레이터가 `--reference-close` **고정값** 주변에서 주문가 살포 → 실시장 굴곡 미반영 | 실 klines에 **앵커링**(가격·도착률), seed 고정으로 재현성 유지 |
| L3 | 체결가 vs 시장 벤치마크 **비교 지표(슬리피지) 없음** | 방향 분리 슬리피지 + 금액 환산 + 서빙 뷰 + 알람 |
| L4 | **단일 심볼·docker-compose** — 다종목 분석·k8s 운영 부재 | k3d 멀티심볼 운영화 (수집→처리→오케스트레이션→대시보드) |

---

## 2. 목표 / 비목표

### 2.1 Goals
- **G1** `market_hourly_summary`에 **VWAP 벤치마크** — 실 체결(`processed_trades`) 기반, 신규 원천 없음.
- **G2** 시뮬레이터 **실시장 앵커링** — 주문 시각의 실 klines close에 가격 앵커, 실 거래량에 도착률 앵커. **seed + 픽스처 해시로 결정적 재현**.
- **G3** **방향 분리 슬리피지** 지표 + `execution_vs_market` 서빙 뷰 + Grafana 알람. 정직한 포지셔닝 유지(§2.3).
- **G4** **k3d 멀티심볼 운영화** — 다심볼 수집·처리·오케스트레이션(Airflow KubernetesExecutor)·심볼 템플릿 대시보드.

### 2.2 Non-Goals
- **ML 시장 이상탐지 / MLOps 재학습 루프** — 보류(ROADMAP §7).
- **Flink 비교 벤치** — 보류(ROADMAP §8.2).
- **실매매 / 주문 실행 / 가격 예측 / 전략 수익률 주장.**
- **합성 주문 슬리피지를 "실행 스킬"로 주장** — 무작위 산포 합성인 한 슬리피지는 **분포 통계**다(§2.3).
- **클라우드 k8s(GKE 등)** — 로컬 k3d로 한정. 오퍼레이터(Strimzi/Spark Operator)도 1차 비도입.

### 2.3 핵심 원칙 (crux — 지표가 무의미해지지 않도록)
- **앵커 기준점 ≠ 벤치마크 기준점 (순환 방지, 불변 조건)**: 앵커는 주문 시각의 **minute close**(의사결정 시점 가격), 슬리피지 벤치마크는 **구간 VWAP** — 반드시 다른 시계열. 같은 VWAP에 앵커링하고 같은 VWAP으로 재면 기대 슬리피지 ≈ 0의 **자기 되읽기 순환**이 된다. 앵커 소스 = 벤치마크 소스가 되는 순간 슬리피지 무효.
- **앵커링은 필요조건이지 충분조건이 아니다 (정직한 포지셔닝)**: 앵커링해도 주문은 무작위 산포 합성이다. 이 단계 슬리피지의 정당한 용도는 (1) 파이프라인·지표의 **기계적 검증**, (2) 실주문/전략 연결 시 그대로 쓸 **인프라 선구축** — 둘뿐. "전략 성과 정량화"는 실데이터 연결 이후에만 주장.
- **실데이터 지표 먼저**: 위험 0인 VWAP(G1) → 앵커링(G2) → 슬리피지(G3) → 운영화(G4) 순서 강제.
- **정직 원칙 계승(v1~v2)**: 검증 전 성능 미주장. 합성 데이터는 실데이터로 위장하지 않으며, 앵커 출처를 `simulated_parameters`에 자기 기술한다.

---

## 3. 아키텍처

기존 v2 흐름(Kafka→Spark→Iceberg→Athena/Grafana)을 **그대로 유지**하고 최소 침습으로 확장한다.

- **Gold 확장**: 신규 파이프라인 없이 기존 두 요약 테이블에 컬럼·조인만 추가(증분 MERGE·MOR 설계 유지). 시뮬레이터는 앵커 픽스처를 읽는 경로만 추가(기존 `--reference-close` 하위 호환).
- **운영화**: 컴포넌트 자체는 그대로, 실행 환경을 docker-compose → k3d로 감싼다. Kafka는 StatefulSet+PVC(브로커 수명 분리), Spark는 `spark-submit --master k8s://`, Airflow는 KubernetesExecutor. 데이터 모델은 이미 `(summary_hour, symbol)` 키·`--symbols` 복수 구독을 지원해 멀티심볼 준비 완료.

---

## 4. 기능 요구사항 (FR)

### 4.1 Gold 실행성과
- **FR-1** `market_hourly_summary.vwap` = `Σ(quote_qty)/NULLIF(Σ(qty),0)` (거래 공백 시 NULL). `06_build_market_hourly_summary_window.py` 확장.
- **FR-2** 시뮬레이터 앵커링: `--anchor-klines` 픽스처 로드, 주문가는 해당 minute close ±편차에서 샘플링, 도착률은 minute 실 거래량 비례 가중. `anchor_file`·`anchor_sha256`·`anchor_mode`를 `simulated_parameters`에 기록. seed 고정 시 바이트 동일 재현.
- **FR-3** `order_execution_summary`에 방향 분리 슬리피지: `benchmark_vwap`, `avg_buy_fill_price`, `avg_sell_fill_price`, `buy_slippage_bps`, `sell_slippage_bps`, `slippage_cost_quote`. `07_...` 잡에서 `market_hourly_summary`를 `(summary_hour, symbol)` LEFT JOIN. (양수=유리 부호 규약)
- **FR-4** `execution_vs_market` Athena 서빙 뷰 — 시황·실행·슬리피지를 한 행에서 cross-reference.
- **FR-5** Grafana 슬리피지 추세(bps)·비용(quote) 패널 + 임계 초과 알람(기존 alerting provisioning 확장).
- **FR-6** DAG 의존성 엣지 `build_market_hourly_summary >> build_order_execution_summary` 명시(현재 엣지 부재, 순서만 우연히 지켜짐).

### 4.2 k3d 멀티심볼 운영화
- **FR-7** k3d 클러스터 + Kafka StatefulSet(PVC) + 멀티심볼 ingestor Deployment. `config/symbols.yaml`을 ConfigMap 단일 소스로 주입, 심볼셋 샤드 단위 배포.
- **FR-8** Spark on k8s: 이미지 로컬 레지스트리 push + `spark-submit --master k8s://`(RBAC/Secret), 체크포인트 S3 격리 유지. 01~09 윈도우 잡 k8s Pod 완주.
- **FR-9** Airflow KubernetesExecutor: 태스크당 Pod, `docker exec` BashOperator → k8s Pod 태스크 전환, gap 감지 → **자동 백필 DAG**.
- **FR-10** 심볼별 앵커 픽스처(G2 스크립트 재사용) + 심볼별 시뮬레이터 Job + Grafana symbol 템플릿 변수. 다심볼 VWAP·슬리피지를 한 대시보드에서 조회.

---

## 5. 비기능 요구사항 (NFR)
- **멱등성**: 모든 윈도우 잡은 `--start-ts/--end-ts/--run-id`로 재실행 안전. 같은 윈도우 MERGE 재실행 시 값 불변.
- **재현성**: 시뮬레이터는 seed + 앵커 픽스처 sha256으로 바이트 동일 재현. 앵커 출처가 데이터에 자기 기술.
- **순환 부재**: 슬리피지 분포가 0 주변 퇴화가 아니라 구간 변동성과 상관을 가짐(anchor≠benchmark 준수 증거).
- **비용 가드**: k3d 로컬 무료, 오퍼레이터 비도입(플레인 매니페스트+spark-submit로 시작), Pod TTL, 심볼 3~5개 상한.
- **격리**: 브로커 수명(PVC) vs 체크포인트(S3) vs 잡 수명(Pod TTL) 분리 — v1 W1 교훈 일반화.
- **롤백 안전**: docker-compose 스택은 k8s 검증(행수/합계 동일) 전까지 유지.

---

## 6. 마일스톤

상세 작업·완료 기준은 `docs/ROADMAP.md`의 Phase 표를 정본으로 한다.

| Phase | 내용 | 의존성 | 대응 FR |
|---|---|---|---|
| **G** | VWAP 벤치마크 | 없음(실데이터) | FR-1 |
| **A** | 시뮬레이터 실시장 앵커링 | 없음 | FR-2 |
| **X** | 방향 분리 슬리피지 + 서빙 뷰 + 알람 | **G + A 완료 필수** | FR-3~6 |
| **K** | k3d 멀티심볼 운영화 (K1~K4) | G/A/X와 독립, 마지막 | FR-7~10 |
| (보류) | ML 이상탐지·MLOps / Flink | K 이후 재평가 | ROADMAP §7·§8.2 |

---

## 7. 성공 지표
- **G/X**: VWAP·슬리피지 수기 검증 일치, 재실행 멱등성 100%, 순환 부재 검증 통과, 슬리피지 알람 동작 1회 이상.
- **A**: 같은 seed 바이트 동일 재현, 주문 분포와 실 volume 상관 > 0.7, 앵커 소스가 minute close임을 확인.
- **K**: k8s에서 전 파이프라인 완주(compose와 결과 동일), 자동 백필 gap 복구 1회, 3심볼 E2E 대시보드.

---

## 8. 리스크 & 대응 (요약 — 상세는 ROADMAP §6)
| 리스크 | 대응 |
|---|---|
| 순환논리(anchor=benchmark) | 앵커=minute close, 벤치마크=구간 VWAP 강제. A·X 완료 기준에 순환 부재 검증 |
| 합성 슬리피지를 "전략 성과"로 과장 | 분포 통계로만 포지셔닝. 용도=기계적 검증+인프라 선구축으로 한정 |
| 슬리피지를 앵커링 전에 서빙 | 순서 강제(G→A→X). X는 A 완료 전 착수 금지 |
| k3d 범위 폭주 | K1~K4 각자 완료 기준. 오퍼레이터·클라우드 k8s 비도입 |
| 범위 재확대(ML) | Non-Goal로 못박음. K 이후 재평가 |

---

## 9. 참고
- `docs/ROADMAP.md` — Phase G/A/X/K 실행 계획 (정본), 보류(ML/Flink) 요약
- `docs/gold_serving_improvement_plan.md` — Gold 서빙 개선 상세 설계(DDL·집계 SQL·순환 방지 §4.1.1·정직 포지셔닝 §4.1.2)
- `prd-v2` 태그 — v2 스펙·as-built 문서·lag 벤치마크 실측(p95 25.6s→12.9s) 프리즈
- `prd-v1` 태그 — v1 원본
