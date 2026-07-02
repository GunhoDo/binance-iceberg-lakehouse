# Deep Interview Spec: Binance Lakehouse v2 — 스트리밍 lag 최적화 벤치마크 중심 재편

## Metadata
- Interview ID: di-binance-v2-20260702
- Rounds: 9 (+ Round 0 topology)
- Final Ambiguity Score: ~18% (PASSED, threshold 20%)
- Type: brownfield (전신: `binance-iceberg-lakehouse`, 참조: `docs/ROADMAP.md`)
- Generated: 2026-07-02
- Threshold: 0.20
- Initial Context Summarized: no
- Status: PASSED

## Clarity Breakdown
| Dimension | Score | Weight | Weighted |
|-----------|-------|--------|----------|
| Goal Clarity | 0.88 | 0.35 | 0.31 |
| Constraint Clarity | 0.82 | 0.25 | 0.21 |
| Success Criteria | 0.75 | 0.25 | 0.19 |
| Context Clarity | 0.80 | 0.15 | 0.12 |
| **Total Clarity** | | | **0.82** |
| **Ambiguity** | | | **0.18** |

## Topology
| Component | Status | Description | Coverage / Deferral Note |
|-----------|--------|-------------|--------------------------|
| C1 비즈니스 가치 정의 | active | 자소서/면접 대표 서사 재료 생산 | 헤드라인=DE 파이프라인 정합성. centerpiece=스트리밍 end-to-end lag 최적화 벤치마크 |
| C2 성과 측정 체계 | active | before→after 수치를 재현 가능하게 기록 | p95 end-to-end lag ablation 벤치마크(레버별 기여도 분해) |
| C3 이상탐지 + 운영 가시성 | active | 순수 DE 데이터 품질 이상 + 관측 | lag 지표가 freshness SLA 이상탐지로 연결, Grafana + 알림 |
| (드롭) 시장/ML 이상탐지 | deferred | ROADMAP P4/P5 (AI 플랫폼) | 헤드라인 일관성을 위해 명시적 제외 (Round 8 확정) |
| (드롭) k8s/k3d 운영화 | deferred | ROADMAP P2 | centerpiece가 k8s 불요. docker-compose 유지 (Round 9 확정) |

## Goal
**이 프로젝트 하나로 데이터 엔지니어 자소서/면접의 대표 서사를 만든다.** 헤드라인은 "재처리해도 흔들리지 않는 실시간 데이터 파이프라인 정합성(DE)"이며, 그 위에 **가장 자랑할 한 방(centerpiece)으로 "스트리밍 end-to-end lag를 baseline→optimized로 개선한 성능 벤치마크"**를 둔다. 이 벤치마크는:
- **가장 어려운 문제** — 분산 스트리밍 파이프라인의 지연 최적화
- **실무와 가장 가까운** — Kafka→Spark Structured Streaming→Iceberg 실운영 튜닝
- **수치로 드러남** — p95 lag before→after, 레버별 기여도 ablation 표

트레이딩 시뮬레이션/가격예측/ML은 헤드라인이 아니며 이 스코프에서 제외한다.

## Constraints
- **헤드라인 고정**: DE 파이프라인 정합성. quant/트레이딩/가격예측을 헤드라인으로 삼지 않는다.
- **벤치마크 부하는 통제된 리플레이**: baseline vs optimized를 "정확히 같은 초당 X건" 부하에서 비교. 리플레이는 재실행 가능해야 한다(재현성).
- **실시간 서사는 WebSocket**: Binance 공개 WebSocket(aggTrade/kline/depth, 무인증·무료, 5 msg/s·1024 stream 제한)으로 실시간 수집 + 품질 이상탐지 데모.
- **이상탐지는 순수 DE(규칙/통계 기반)**: ML 불요.
- **k8s/k3d 제외**: docker-compose 유지. (초기 k3d 언급은 이 centerpiece에 불필요하다고 판정)
- **lag 정의**: end-to-end lag = (Iceberg commit 시각 − 거래소 event 시각)의 p50/p95/p99, 명시된 throughput에서 측정.
- **정직 원칙 유지**: 검증 전 성능 미주장. 합성/가정 데이터는 실데이터로 위장하지 않는다(기존 프로젝트 원칙 계승).

## Non-Goals
- 시장 이상탐지 / 가격예측 / 트레이딩 전략 성과(수익률) 주장
- ML 모델·MLOps 재학습 루프 (ROADMAP P4/P5)
- k8s/k3d 운영화 (ROADMAP P2)
- 멀티 거래소 일반화
- 대용량 "저장" 자랑 (핵심은 저장량이 아니라 lag 최적화 수치)

## Acceptance Criteria
- [ ] Binance WebSocket ingestor가 aggTrade/kline을 이벤트 타입별 Kafka 토픽으로 발행(재연결·백오프 포함)
- [ ] 통제된 리플레이 하니스: 과거 덤프를 "정확히 초당 N건"으로 재생, 동일 조건 재실행 가능
- [ ] end-to-end lag 계측 코드: event_time→kafka→spark→iceberg commit 각 구간 타임스탬프 기록, p50/p95/p99 산출
- [ ] ablation 벤치마크: baseline → +트리거/배치 튜닝 → +쓰기(커밋) 경로 → +병렬성 순으로 각 단계 p95 lag·throughput을 표로 기록
- [ ] 최종 산출: "동일 부하 N msg/s에서 p95 lag X초 → Y초 (Z% 감소)" + 레버별 기여도 표
- [ ] 데이터 품질 이상탐지: freshness SLA 위반·gap(결측 캔들)·스키마 드리프트·순서역전·NULL/0 감지 → `anomaly`/`quality_events` 적재
- [ ] lag 지표가 freshness SLA 이상탐지로 연결(임계 초과 시 알람)
- [ ] Grafana 대시보드: end-to-end lag(p50/p95/p99), 토픽 적체, 이상 건수, 잡 성공/실패
- [ ] 운영 가시성: 잡 실행 이력·에러가 관측 테이블 + 대시보드로 남고, 알림 채널(Discord webhook 등, graceful degrade) 연동
- [ ] 벤치마크 재현성: 리플레이·튜닝 파라미터가 커밋되어 제3자가 동일 결과 재생 가능

## Assumptions Exposed & Resolved
| Assumption | Challenge | Resolution |
|------------|-----------|------------|
| 트레이딩 시뮬로 비즈니스 성과(수익률)를 증명한다 | quant/ML 헤드라인은 DE 정체성을 흐린다 (ROADMAP Non-Goal) | 헤드라인=DE 정합성으로 고정, 트레이딩은 스코프에서 제외 |
| "대용량 처리 최적화"가 대표 서사다 | 현재 데이터는 1개월·1심볼 정적 덤프라 대용량 주장 불가 | "대용량"은 예시일 뿐, 진짜 목표는 "어려운 문제+실무+수치". centerpiece를 lag 벤치로 재정의 |
| centerpiece는 W1 정합성 fix여야 (권장) | 사용자는 성능 벤치를 선호 | 성능 벤치(end-to-end lag)로 확정. W1은 선택적 보조 |
| lag는 실시간 WebSocket으로 측정 | 실시간 부하는 들쭉날쭉해 baseline↔optimized 비교가 오염됨 | 벤치=통제 리플레이, 실시간 서사=WebSocket으로 역할 이원화 |
| 최적화 레버는 하나면 된다 | 단일 레버는 기여도 설명이 약함 | ablation(다중 레버 조합)으로 레버별 기여도 분해 |
| 이상탐지에 ML(AI 플랫폼)을 넣는다 | 헤드라인이 DE인데 ML은 서사 분산 | 순수 DE 데이터 품질 이상만. ML 드롭 |
| k3d/k8s로 운영화한다 | centerpiece(lag 벤치)는 k8s 불요, 효과 대비 비용 큼 | k8s 제외, docker-compose 유지 |

## Technical Context (brownfield 근거)
- **기존 자산**: Kafka(KRaft)→Spark Structured Streaming→Iceberg(Glue/S3)→Airflow→Grafana. `src/streams/stream_raw_*.py`, 관측 테이블 3종(`pipeline_run_summary`,`data_quality_summary`,`table_health_summary`), `experiments/*.ipynb`(compaction·MOR·manifest 실험).
- **git log 실측 근거**: `processed_trade optimization`, `daliy job lightweight`, `prevent for OOM`, `solve memory chash in ec2` — 실제 최적화·OOM 해결 이력 존재하나 **실측 수치 미기록**(이력서도 "대규모 부하 실측 못 함" 인정). → centerpiece 작업 = 이 최적화를 통제 부하 위에서 엄밀히 벤치마킹하는 것.
- **신규 필요**: (1) Binance WebSocket 클라이언트(현재 없음 — `download_data.sh`는 2024-01 정적 덤프 wget, `csv_to_kafka.py`는 리플레이), (2) 통제 리플레이 하니스(초당 rate 고정), (3) end-to-end lag 계측·집계, (4) ablation 러너, (5) freshness/gap/schema 품질 이상탐지, (6) lag/이상 Grafana 패널.
- **Binance API 사실확인**: 공개 WebSocket 스트림(aggTrade/kline/depth)은 무인증·무료. 연결당 최대 1024 stream, 5 incoming msg/s 초과 시 disconnect.

## Ontology (Key Entities)
| Entity | Type | Fields | Relationships |
|--------|------|--------|---------------|
| LagMetric | core | event_time, kafka_ts, spark_ts, iceberg_commit_ts, p50/p95/p99, throughput | AblationBenchmark가 측정, QualityAnomaly(freshness)가 소비 |
| AblationBenchmark | core | levers[], baseline_p95, optimized_p95, per_lever_contribution | LagMetric을 baseline↔optimized로 비교 |
| DataSource | core | replay(controlled rate), websocket(live) | Ingestor/리플레이가 Kafka 토픽으로 발행 |
| QualityAnomaly | core | kind(freshness/gap/schema/order/null), threshold, detected_at | LagMetric(freshness) 소비, `anomaly` 테이블 적재 |
| ObservabilitySurface | supporting | grafana_panels, run_history, alert_channel | LagMetric·QualityAnomaly·RunLog 시각화 |

## Ontology Convergence
| Round | Entity Count | New | Changed | Stable | Stability Ratio |
|-------|-------------|-----|---------|--------|----------------|
| 1 | 8 | 8 | - | - | N/A |
| 2 | 8 | 2 | 3 | 3 | 60% |
| 3 | 7 | 1 | 2 | 4 | 75% |
| 4 | 6 | 1 | 1 | 4 | 82% |
| 5 | 6 | 1 | 0 | 5 | 88% |
| 6 | 5 | 0 | 1 | 4 | 90% |
| 7 | 5 | 1 | 0 | 4 | 92% |
| 8 | 5 | 0 | 1 (ML 제거) | 4 | 95% |
| 9 | 5 | 0 | 0 | 5 | 95% |

## Interview Transcript
<details>
<summary>Full Q&A (9 rounds + topology)</summary>

- **R0 Topology**: 3축(비즈니스 가치 / 측정 체계 / 이상탐지+가시성) 확정.
- **R1 헤드라인**: → 파이프라인 정합성 DE (트레이딩은 다운스트림).
- **R2 트레이딩 시뮬 역할**: → 재정의: "이 프로젝트 하나로 대표 서사(대용량 최적화·AI 플랫폼·성능 개선)를 커버하고 싶다".
- **R3 스케일 소스**: → 재정의: "대용량은 예시일 뿐, 진짜는 가장 어려운 문제+실무+수치".
- **R4 centerpiece (Contrarian)**: → 성능 최적화 벤치마크.
- **R5 벤치 축**: → 스트리밍 end-to-end lag.
- **R6 최적화 레버 (Simplifier)**: → 여러 레버 조합 실험(ablation).
- **R7 부하 소스**: → 이원화(통제 리플레이=벤치, WebSocket=실시간).
- **R8 이상 정의 (Ontologist)**: → 데이터 품질 이상 (순수 DE, ML 드롭).
- **R9 빌드 범위**: → 벤치+품질이상+관측 (k8s 제외).

</details>

---

## ROADMAP(향후 확장) 대비 차이 (요약)
| 항목 | ROADMAP (향후 확장) | 현재 PRD (v2) |
|---|---|---|
| 헤드라인 | 실시간 파이프라인 + 정식 fix + k8s | 실시간 파이프라인 + **end-to-end lag 최적화 벤치마크** |
| centerpiece | W1 정식 fix(P1) | **스트리밍 lag ablation 벤치마크** (W1은 선택적 보조) |
| k8s(P2) | 핵심 축 | **제외** |
| ML 이상탐지(P4/P5) | 도전 목표 | **제외**, 데이터 품질 이상만 |
| 데이터 소스 | WebSocket + REST 백필 | WebSocket(실시간) + **통제 리플레이(벤치)** 이원화 |
| 대표 수치 | 재처리 멱등성 100% | **p95 lag X→Y초 + 레버별 기여도 표** |
