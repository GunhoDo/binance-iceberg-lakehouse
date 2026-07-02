# PRD — Binance Lakehouse v2: 실시간 수집 + 스트리밍 lag 최적화 벤치마크

> 대상 직무: 데이터 엔지니어
> 버전 계보: `binance-iceberg-lakehouse` v1(원본, `prd-v1` 태그로 보존) → **현재 PRD = v2**. k8s·ML을 포함한 광의 확장 비전은 버전이 아니라 `docs/ROADMAP.md`로 분리했다.
> **v2의 결정**: 초기 확장안(현 ROADMAP)이 담으려던 k8s·ML 이상탐지를 잘라내고, **"스트리밍 end-to-end lag 최적화 벤치마크"** 하나에 집중한다.
> 설계 원칙: **헤드라인은 DE 파이프라인 정합성.** 트레이딩/가격예측/ML은 스코프에서 제외한다. 대표 서사는 "가장 어려운 문제 + 실무와 가장 가까운 경험 + 수치로 드러나는 성과"를 모두 만족하는 **lag 최적화 벤치마크**다.

---

## 0. 스코프 결정 (확장안 대비 무엇을 좁혔나)

`docs/ROADMAP.md`는 실시간化·정식fix·k8s·ML·MLOps를 한 번에 담은 광의 확장안이다. 이 PRD(v2)는 그중 검증 가능한 한 방에 집중하기 위해 다음과 같이 좁혔다.

| 항목 | 확장안 (현 ROADMAP) | 현재 PRD (v2) |
|---|---|---|
| 헤드라인 | 실시간 파이프라인 + 정식 fix + k8s | 실시간 파이프라인 + **end-to-end lag 최적화 벤치마크** |
| centerpiece | W1 정식 fix | **스트리밍 lag ablation 벤치마크** (W1은 선택적 보조 서사) |
| k8s | 핵심 축 | **제외** — docker-compose 유지 |
| ML/시장 이상탐지 | 도전 목표 | **제외** — 데이터 품질 이상만 (순수 DE) |
| 데이터 소스 | WebSocket + REST 백필 | **WebSocket(실시간) + 통제 리플레이(벤치마크)** 이원화 |
| 대표 수치 | 재처리 멱등성 100% | **p95 end-to-end lag X초→Y초 + 레버별 기여도 표** |

**왜 좁혔나**: 광의 확장안은 범위가 과대해 면접 대비 관점에서 "가장 자랑할 한 방"이 분산된다. 이 PRD는 *하나의 검증 가능한 수치*(lag 개선)에 집중해 서사 밀도를 높인다.

---

## 1. 배경 & 문제 정의

### 1.1 현재 상태 (v1)
- 체결·캔들은 Binance 공개 데이터(**2024년 1월 BTCUSDT 정적 덤프**), 주문은 시뮬레이션 데이터.
- Kafka(KRaft) → Spark Structured Streaming → Iceberg → Airflow → Athena/Grafana. 단일 머신, Docker Compose.
- git 이력상 `processed_trade optimization`, `daliy job lightweight`, `prevent for OOM`, `solve memory crash in ec2` 등 **실제 최적화·OOM 대응 경험은 있으나 실측 수치를 남기지 못했다** (README/이력서도 인정).

### 1.2 이 PRD(v2)가 해결하는 것
| # | 한계 | v2에서의 해결 |
|---|---|---|
| H1 | 최적화를 했지만 **before→after 수치가 없음** | 통제 부하 위에서 **end-to-end lag ablation 벤치마크**로 레버별 기여도까지 수치화 |
| H2 | 데이터가 정적이라 "실시간"이 아님 | Binance **WebSocket 실시간 수집** 추가 |
| H3 | 벤치마크 부하가 비재현적이면 비교가 오염됨 | **통제 리플레이 하니스**(초당 rate 고정, 재실행 가능)로 baseline↔optimized 공정 비교 |
| H4 | 품질 이상을 코드 밖에서 감지 못함 | freshness SLA·gap·스키마·순서역전·NULL 규칙 기반 **데이터 품질 이상탐지** + Grafana |

---

## 2. 목표 / 비목표

### 2.1 Goals
- **G1** Binance 공개 WebSocket으로 실시간 시장 데이터를 수집(aggTrade/kline)하는 스트리밍 파이프라인.
- **G2** **통제 리플레이 하니스**: 과거 덤프를 "정확히 초당 N건"으로 재생, 동일 조건 재실행 가능.
- **G3** **end-to-end lag 계측**: (Iceberg commit 시각 − 거래소 event 시각)의 p50/p95/p99를 명시된 throughput에서 산출.
- **G4** **ablation 벤치마크**: baseline → +트리거/배치 튜닝 → +쓰기(커밋) 경로 → +병렬성 순으로 각 레버의 lag 기여도를 표로 분해.
- **G5** **데이터 품질 이상탐지**(순수 DE): freshness SLA·gap·스키마 드리프트·순서역전·NULL 감지 → `anomaly`/`quality_events` 적재.
- **G6** **운영 가시성**: lag(p50/p95/p99)·이상 건수·잡 성공실패를 Grafana + 알림(Discord webhook 등, graceful degrade)으로 노출.

### 2.2 Non-Goals
- **실제 매매/주문 실행**, 트레이딩 전략 성과(수익률) 주장, 가격 예측.
- **ML/시장 이상탐지 및 MLOps 재학습 루프** (ROADMAP으로 이관).
- **k8s/k3d 운영화** (ROADMAP으로 이관) — docker-compose 유지.
- 멀티 거래소/멀티 자산 일반화.
- 대용량 "저장량" 자랑 (핵심은 저장량이 아니라 lag 최적화 수치).

---

## 3. 아키텍처

### 3.1 목표 데이터 흐름
```
Binance 공개 API
 └─ WebSocket (aggTrade / kline_1m)  ── 실시간 수집 + 품질 이상탐지 데모

과거 덤프 (data.binance.vision)
 └─ 통제 리플레이 하니스 (초당 N건 고정, 재실행 가능)  ── 벤치마크 부하
        │
        ▼  [둘 다 Kafka 토픽으로 발행]
   Kafka (KRaft)  ── 토픽: trades / klines / quality_events
        │
        ▼  [Spark Structured Streaming] ── 각 구간 타임스탬프 기록
   정제·중복제거·워터마크 → Iceberg MERGE(klines) / append(trades)
        │
        ├─► [lag 계측] event_time→kafka→spark→iceberg commit → p50/p95/p99
        │        ▼
        │   [ablation 러너] baseline vs +levers → 레버별 기여도 표
        │
        └─► [품질 이상탐지] freshness SLA·gap·schema·order·null → anomaly 테이블
                 ▼
            Grafana (lag·적체·이상·잡 상태) + 알림(Discord webhook)
```

### 3.2 벤치마크 부하를 왜 리플레이로 두나
엄밀한 before→after 비교는 **통제된·재현 가능한 부하**를 요구한다. 실시간 WebSocket은 시장 상황에 따라 초당 메시지 수가 들쭉날쭉해 baseline과 optimized를 서로 다른 부하에서 재게 되어 비교가 오염된다. 따라서 **벤치마크 숫자는 통제 리플레이로, "실시간"·품질 이상탐지 데모는 WebSocket으로** 역할을 이원화한다.

---

## 4. 기능 요구사항 (FR)

### 4.1 수집 (Ingestion)
- **FR-1** WebSocket ingestor: `aggTrade`·`kline_1m` 구독 → 이벤트 타입별 Kafka 토픽 발행. 재연결(지수 백오프)·하트비트. (공개 스트림: 무인증, 5 msg/s·1024 stream 제한 준수)
- **FR-2** 통제 리플레이 하니스: 과거 덤프를 지정 rate(초당 N건)로 Kafka에 발행. rate·구간·seed를 파라미터화해 재실행 시 동일 부하 재현.

### 4.2 처리·적재 (Processing)
- **FR-3** Spark Structured Streaming: Kafka 구독 → 파싱·중복제거·워터마크 → Iceberg. 각 단계 처리 타임스탬프 기록.
- **FR-4** 멱등 적재: klines는 `(symbol, open_time)` MERGE, trades는 append+dedup. 동일 구간 재처리 시 중복 0 (v1 원칙 계승, 벤치마크 재실행 안전성의 전제).

### 4.3 성능 측정 (Measurement) — **centerpiece**
- **FR-5** end-to-end lag 계측: `iceberg_commit_ts − exchange_event_ts`의 p50/p95/p99를 명시된 throughput에서 산출·기록.
- **FR-6** ablation 벤치마크 러너: 레버 조합(트리거/배치 튜닝 → 쓰기 경로 → 병렬성)을 순차 적용하며 각 단계의 p95 lag·throughput을 표로 기록. baseline은 개선 여지 확보를 위해 의도적으로 naive 설정에서 시작.
- **FR-7** 산출물: "동일 부하 N msg/s에서 p95 lag X초 → Y초 (Z% 감소)" + 레버별 기여도 표. 리플레이·튜닝 파라미터를 커밋해 제3자 재현 가능.

### 4.4 이상탐지 — 데이터 품질 (순수 DE)
- **FR-8** 프레시니스 SLA: 토픽별 최신 이벤트 지연(= lag)이 임계 초과 시 이상 적재 + 알람. (lag 지표 재사용)
- **FR-9** gap(결측 캔들 open_time 불연속)·스키마 드리프트·순서역전·NULL/0 감지 → `anomaly`/`quality_events` 적재.

### 4.5 관측 (Observability)
- **FR-10** Grafana: end-to-end lag(p50/p95/p99), 토픽 적체, 적재 행수, 이상 건수, 잡 성공/실패.
- **FR-11** 관측 테이블(v1 자산 계승): `pipeline_run_summary`·`data_quality_summary`·`table_health_summary` + 알림 채널(Discord webhook, 미설정 시 graceful degrade).

---

## 5. 비기능 요구사항 (NFR)
- **재현성**: 벤치마크는 리플레이 rate·튜닝 파라미터·seed 고정으로 "동일 결과 재생 가능". 이것이 이 PRD 성과 서사의 신뢰 근거.
- **멱등성**: 배치/스트리밍 잡은 재실행 안전 (v1 원칙 계승).
- **정직성**: 검증 전 성능 미주장. 목표 lag 수치(X→Y)는 실측 후 확정. baseline은 조작 없이 실제 naive 설정으로.
- **비용**: docker-compose 로컬/단일 EC2. k8s 미도입으로 운영 복잡도·비용 최소화.

---

## 6. 마일스톤

| Phase | 내용 | 핵심 산출물 |
|---|---|---|
| **P0** 실시간化 | WebSocket ingestor + Kafka 토픽 | 실시간 수집 데모 |
| **P1** 리플레이 하니스 | 초당 rate 고정 리플레이 | 재현 가능한 벤치 부하 |
| **P2** lag 계측 | 구간별 타임스탬프 + p50/p95/p99 | lag 대시보드 |
| **P3** ablation 벤치 (**핵심**) | 레버별 튜닝·측정 | **레버별 기여도 표 + X→Y초 개선** |
| **P4** 품질 이상탐지 | freshness/gap/schema/order/null | `anomaly` 테이블 + Grafana 알람 |
| **P5** 관측 마감 | Grafana 패널 + 알림 연동 | 운영 가시성 완성 |

> **P0~P3가 진짜 핵심.** P3(ablation 벤치)가 대표 서사다. P4~P5는 그 위에 얹는 DE 정합성·운영 가치.

---

## 7. 성공 지표
- **대표 수치**: 동일 부하에서 **p95 end-to-end lag X초 → Y초 (Z% 감소)**, 레버별 기여도 분해 표.
- **재현성**: 커밋된 파라미터로 제3자가 동일 벤치 결과 재생.
- **품질**: freshness SLA 준수율, gap 자동 감지율, 이상 알람 정확도.
- **정합성**: 재처리 멱등성(중복 0) — 벤치 재실행 안전성의 전제이자 보조 서사.

---

## 8. 리스크 & 대응
| 리스크 | 대응 |
|---|---|
| baseline이 이미 빨라 개선폭이 작음 | P3에서 baseline을 의도적으로 naive 설정에서 출발(조작 아님, 실제 기본값) |
| "대용량 아님" 지적 | 핵심은 저장량이 아니라 lag 최적화. 통제 부하 rate를 명시해 방어 |
| 실시간 부하 비재현성 | 벤치는 리플레이로 통제, WebSocket은 실시간 데모로 분리 |
| 범위 재확대 유혹(ML/k8s) | Non-Goal로 못박음. 서사 밀도를 위해 잘라낸 결정 유지 |
| Binance API rate limit | 공개 스트림만, 백오프. 5 msg/s·1024 stream 준수 |

---

## 9. 이 프로젝트가 입증하는 역량
- **성능 엔지니어링**: 스트리밍 end-to-end lag를 통제 부하 위에서 ablation으로 분해·최적화하고 수치로 증명.
- **측정 설계**: baseline↔optimized 공정 비교를 위한 재현 가능한 벤치마크 방법론.
- **운영 정합성**: 멱등 적재·재처리 안전성으로 벤치 재실행 신뢰성 확보.
- **관측성**: lag·품질 이상·잡 상태를 Grafana + 알림으로 코드 밖에서 감지.

---

## 10. 참고
- 상세 인터뷰 스펙: `docs/deep-interview-scope-v2.md`
- 향후 확장 로드맵: `docs/ROADMAP.md` (k8s/ML 포함 광의안 — v2에서 제외)
