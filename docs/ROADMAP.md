# ROADMAP — Binance Lakehouse 향후 확장 (k8s 운영화 + 스트리밍 이상탐지/ML)

> ⚠️ 이 문서는 **현재 스코프가 아닌 향후 확장 비전**이다. 현재 정본은 `docs/PRD.md`.
> 여기 담긴 k8s(P2)·ML 이상탐지(P4/P5)는 PRD에서 의도적으로 제외하고 로드맵으로 보존한 항목이다.
> (원문은 PRD v2로 작성한 광의 확장안이며, 명칭만 ROADMAP으로 재정리했다.)

> 대상 직무: 데이터 엔지니어
> 전신: `binance-iceberg-lakehouse` (정적/배치 → 실시간/운영화로 확장)
> 설계 원칙: **ML은 파이프라인의 다운스트림 소비자다.** 주인공은 데이터 파이프라인(정합성·멱등성·관측성), ML은 그 위에 얹는 가치. 이 PRD는 "DE 정체성"을 흐리지 않는 선에서 MLOps를 더한다.

---

## 1. 배경 & 문제 정의

### 1.1 현재 상태 (v1)
- 체결·캔들은 Binance 공개 데이터(**정적/과거 덤프**), 주문은 시뮬레이션 데이터.
- Kafka(KRaft) → Spark Structured Streaming → Iceberg → Airflow → Athena/Grafana.
- **단일 머신**, Docker Compose 기반.

### 1.2 v1의 한계와 v2 개선 목표
| # | v1 한계 | v2에서의 해결 |
|---|---|---|
| W1 | Kafka offset ↔ Spark 체크포인트 불일치 장애를 **로컬 재실행으로 우회**(정식 fix 부재) | 브로커 수명 분리 + 체크포인트 격리 + 멱등 MERGE로 **정식 fix** |
| W2 | 1년 치 백필이 Airflow가 아니라 **수동 재실행** | REST gap-fill + Airflow KubernetesExecutor **자동 백필** |
| W3 | **k8s 미적용**(단일 머신/Compose) | 전 파이프라인 **k8s 운영화** |
| W4 | 데이터가 정적이라 실시간 운영 부재 | Binance **WebSocket 실시간 수집** |

### 1.3 왜 지금 ML(이상탐지)을 얹나
산업용 AIoT 플랫폼은 보통 "데이터 수집 → AI 분석 → **예측/이상탐지** → 운영 + MLOps"를 하나로 묶는다. 시장 데이터 스트림에 **이상탐지 + 재학습 루프**를 얹으면 그 구조를 *축소판으로 직접 구현해 본 경험*이 된다. 동시에 스트리밍 이상탐지(streaming anomaly detection) 연구 주제와도 직결된다.

---

## 2. 목표 / 비목표

### 2.1 Goals
- **G1** Binance 공개 WebSocket으로 실시간 시장 데이터를 수집하는 스트리밍 파이프라인.
- **G2** 전 구성요소(Kafka/Spark/Airflow/모델 학습 Job)를 **k8s 위에서 운영**.
- **G3** W1 장애의 **정식 fix**: 멱등 + exactly-once 지향 적재(같은 구간 재처리해도 동일 결과).
- **G4** **데이터 품질 이상탐지**(결측·지연·스키마/프레시니스 SLA) — 순수 DE.
- **G5** **시장 이상탐지** 모델: 학습(k8s Job) → 서빙(스트리밍 스코어링) → **재학습 루프(MLOps)**.
- **G6** 이상 발생 시 Iceberg 적재 + Grafana 알람 + Discord 웹훅 알림.

### 2.2 Non-Goals
- **실제 매매/주문 실행** (공개 마켓 데이터만 사용 — 인증·자본·법적 리스크 회피).
- **가격 예측을 헤드라인으로** (정확도 입증 부담 + DE 정체성 희석). → §6.4 stretch goal로만, honest caveat 동반.
- SOTA 모델 추구. **모델은 단순한 것부터**(통계 → IsolationForest → 시퀀스). 모델 정확도가 아니라 *파이프라인+MLOps 루프*가 평가 대상.
- 멀티 거래소/멀티 자산 일반화 (단일 거래소 BTC/ETH 등 소수 심볼로 한정).

---

## 3. 아키텍처

### 3.1 목표 데이터 흐름
```
Binance 공개 API
 ├─ WebSocket  (aggTrade / kline_1m / depth / ticker)  ── 실시간
 └─ REST       (klines 과거 조회)                       ── 백필/gap-fill
        │
        ▼  [ingestor: 컨테이너, k8s Deployment]
   Kafka (KRaft, PVC 영속)   ── 토픽 분리: trades / klines / depth / quality_events
        │
        ▼  [Spark Structured Streaming on k8s]
   정제·중복제거·워터마크 → Iceberg MERGE(klines/state) / append(trades)
        │
        ├─► Iceberg: raw, curated, **feature**, **anomaly**, **ops_metrics** 테이블
        │
        ▼  [feature pipeline (Spark)]  롤링 통계: 수익률·변동성·거래량 z-score·스프레드
        │
        ├─► [모델 학습: k8s Job]  feature 테이블 → 모델 아티팩트 → 레지스트리(MLflow/S3)
        │        ▲ 재학습 트리거: Airflow 스케줄 + 드리프트 감지
        │
        ▼  [추론: 스트리밍 스코어링]  최신 feature → 이상 점수
        │
        └─► Iceberg anomaly 테이블 + Grafana 알람 + Discord 웹훅
   Airflow(KubernetesExecutor): 백필·재학습·품질체크 DAG 오케스트레이션
```

### 3.2 v1 → v2 매핑
| 레이어 | v1 | v2 |
|---|---|---|
| 수집 | 정적 덤프 | WebSocket 실시간 + REST 백필 |
| 실행 환경 | 단일머신/Compose | **k8s** (Deployment/Job/CronJob) |
| 적재 정합성 | offset 우회 | 멱등 MERGE + 체크포인트 격리(정식 fix) |
| 백필 | 수동 | Airflow 자동 + REST gap-fill |
| 분석 | 없음 | 데이터품질 + 시장 이상탐지 |
| MLOps | 없음 | 학습 Job → 레지스트리 → 재학습 루프 |

---

## 4. 기능 요구사항 (FR)

### 4.1 수집 (Ingestion)
- **FR-1** WebSocket ingestor: 심볼별 `aggTrade`·`kline_1m`·`depth`·`ticker` 구독 → 이벤트 타입별 Kafka 토픽 발행. 재연결(지수 백오프)·하트비트.
- **FR-2** REST backfill: 지정 `[start, end]` 구간 klines 조회 → Kafka 또는 직접 Iceberg MERGE. **Airflow DAG에서 호출**(W2 해결).
- **FR-3** **gap detector**: curated klines의 open_time 연속성 검사 → 결측 구간을 quality_events 토픽 + 자동 백필 트리거.

### 4.2 처리·적재 (Processing)
- **FR-4** Spark Structured Streaming: Kafka 구독 → 파싱·중복제거·워터마크(지연 이벤트 허용) → Iceberg.
- **FR-5** **멱등 MERGE**: klines는 `(symbol, open_time)` 키로 MERGE(COW/MOR는 테이블 성격별 분리), trades는 append + dedup. **같은 구간 재처리 시 중복 0**(W1 해결의 핵심).
- **FR-6** 체크포인트는 S3에 **run/토픽 단위로 격리**, Kafka 브로커는 PVC로 영속 → 브로커 재생성과 체크포인트 수명 분리(W1 정식 fix).

### 4.3 피처 (Feature)
- **FR-7** feature 파이프라인: 윈도우 롤링 통계 산출 — 로그수익률, 실현변동성, 거래량 z-score(EWMA), 호가 스프레드, 단위시간 체결수 → Iceberg `feature` 테이블(학습/추론 공용 = train/serve skew 방지).

### 4.4 이상탐지 — 데이터 품질 (DE, §6.1)
- **FR-8** 프레시니스 SLA: 토픽별 최신 이벤트 지연이 임계 초과 시 ops 알람.
- **FR-9** 스키마 드리프트·결측·순서역전 감지 → `quality_events`/`anomaly` 적재.

### 4.5 이상탐지 — 시장 (ML, §6.2)
- **FR-10** 학습 Job(k8s): `feature` 테이블 → 모델 학습 → 아티팩트 + 메타(데이터범위·하이퍼파라미터·메트릭)를 레지스트리에 버전 등록.
- **FR-11** 추론: 최신 feature를 스코어링 → 임계 초과 시 `anomaly` 테이블 + Grafana + Discord.
- **FR-12** 재학습 루프: Airflow 스케줄(예: 일 1회) + **입력 분포 드리프트 감지 시 트리거**. champion/challenger 비교 후 승격.

### 4.6 관측 (Observability)
- **FR-13** Grafana: 처리 지연(end-to-end lag), 토픽 적체, 적재 행수, 이상 건수, 모델 버전/스코어 분포.
- **FR-14** `ops_metrics`·실행이력 테이블(v1 관측 자산 계승·확장).

---

## 5. 비기능 요구사항 (NFR)
- **멱등성**: 모든 배치/스트리밍 잡은 `--start-ts/--end-ts/--run-id`로 재실행 안전(v1 원칙 계승).
- **정합성**: exactly-once 지향(워터마크 + 멱등 MERGE). 재처리 후 행수·합계 불변을 검증 잡으로 확인.
- **재현성**: 모델·데이터 버전 고정(레지스트리 메타에 데이터 범위/seed 기록). "6개월 뒤 같은 결과".
- **비용**: spot/preemptible 노드 + 학습 끝나면 Job 종료(TTL). 유휴 GPU 금지(필요 시 CPU 모델 우선).
- **격리**: 장애 도메인 분리(브로커 vs 체크포인트 vs 모델) — W1 교훈의 일반화.

---

## 6. ML 컴포넌트 상세

### 6.1 데이터 품질 이상탐지 (먼저·확실 — 순수 DE)
규칙/통계 기반, ML 불필요. **가장 먼저 구현**해 "이상탐지 = 데이터 정합성"이라는 DE 서사를 만든다.
- 결측 캔들(open_time gap), 프레시니스 SLA 위반, 가격·거래량 NULL/0, 순서역전, 스키마 변경.
- 산출: `anomaly`(kind=quality) + Grafana 알람. **백필 자동 트리거와 연동**(FR-3).

### 6.2 시장 이상탐지 (메인 ML)
**단순→복잡 단계적 도입.** 모델 자체보다 *학습→서빙→재학습 루프*가 핵심.
1. **통계 베이스라인**: 수익률/거래량의 EWMA z-score, STL 잔차 → 임계 기반. (무학습, 즉시 가치)
2. **고전 ML**: IsolationForest / One-Class SVM (feature 테이블 입력, k8s Job 학습).
3. **(선택) 시퀀스**: 소형 LSTM/오토인코더 재구성오차 — 자원 여유 시.
- **평가**: 정답 라벨이 없으므로 합성 주입 이상(스파이크/플래시크래시 패턴)으로 recall 측정 + 알람율(precision proxy). **검증 전 성능 미주장 원칙** 유지(재현 가능한 근거 없는 성능 주장 금지).

### 6.3 MLOps 루프
- **레지스트리**: MLflow(또는 S3 + 메타 JSON). 모델마다 데이터범위·하이퍼파라미터·메트릭·git sha.
- **재학습**: Airflow DAG가 k8s 학습 Job 제출 → 홀드아웃 평가 → champion보다 좋으면 승격.
- **드리프트**: 입력 feature 분포(PSI/KS) 모니터 → 임계 초과 시 재학습 트리거.

### 6.4 가격 예측 (Stretch — 헤드라인 금지)
- 하려면 **백테스트로만** 검증하고 "투자 신호 아님" 명시. 단기 변동성/방향성 분류(상승/하락/횡보) 정도.
- ⚠️ 정확도 과장 절대 금지. DE 서사를 흐리면 **버린다**.

---

## 7. k8s / 인프라 (경로 A의 실체)
- **Kafka**: StatefulSet + PVC(영속) — W1의 "브로커 수명" 문제 해결.
- **Spark on k8s**: SparkApplication(operator) 또는 spark-submit `--master k8s://`. 체크포인트 S3.
- **Airflow**: KubernetesExecutor — 태스크마다 Pod. 백필/재학습/품질 DAG.
- **모델 학습**: k8s `Job`(표준 Dockerfile/Job/PVC/TTL 패턴). CPU 우선, 필요 시 GPU 1장 spot.
- **클러스터**: 로컬 k3s(무료 연습) → GKE(무료크레딧, 실 배포 1회). 학습 끝나면 노드 축소.
- **비용 가드레일**: spot/preemptible, TTLSecondsAfterFinished, 유휴 노드 축소.

---

## 8. 마일스톤 (단계별 — 각 단계가 독립적으로 "데모 가능")

| Phase | 내용 | 핵심 산출물 | 효과(해결 한계) |
|---|---|---|---|
| **P0** 실시간化 | WebSocket ingestor + Kafka 토픽 분리 | 실시간 수집 데모 | W4 |
| **P1** 정식 fix | 멱등 MERGE + 체크포인트 격리 + 재처리 검증 잡 | "재처리해도 중복 0" 증명 | **W1** |
| **P2** k8s 운영화 | Kafka/Spark/Airflow를 k8s로 + 자동 백필 DAG | k8s 매니페스트 + 자동 백필 | **W2·W3** |
| **P3** 품질 이상탐지 | gap/SLA/스키마 감지 + 알람 | `anomaly(quality)` + Grafana | DE 정합성 강화 |
| **P4** 시장 이상탐지 | feature + 학습 Job + 스트리밍 스코어링 | 모델 + anomaly 테이블 | MLOps 정합 |
| **P5** MLOps 루프 | 레지스트리 + 재학습 + 드리프트 | champion/challenger 승격 데모 | 차별화 |
| (P6) | 가격예측 stretch | 백테스트 리포트(caveat) | 선택 |

> **P0~P2가 진짜 핵심(데이터 엔지니어).** P3~P5는 "그 위에 얹은 가치". P2까지만 해도 v1 한계 3개(W1·W2·W3)가 해결된다 — 무리하면 P2에서 끊어도 완결된다.

---

## 9. 성공 지표
- **DE**: 재처리 멱등성 100%(행수/합계 불변), end-to-end lag < N초, 자동 백필로 gap 자동 복구율, 프레시니스 SLA 준수율.
- **ML**: 합성 주입 이상 recall, 알람율(과알람 비율), 드리프트→재학습 트리거 동작, champion 승격 1회 이상.
- **운영**: k8s에서 무중단 재배포, spot 중단 후 자동 복구, 월 비용 상한 준수.

---

## 10. 리스크 & 대응
| 리스크 | 대응 |
|---|---|
| 범위 과대 → 미완성 | Phase가 각자 데모 가능하게 설계. **P2에서 끊어도 완결**. |
| ML로 무게 쏠려 DE 정체성 희석 | ML은 다운스트림 소비자로 한정. 헤드라인은 항상 "실시간 파이프라인 + 정식 fix + k8s". |
| 가격예측 과장 유혹 | Non-Goal로 못박음. 백테스트·caveat 없이는 언급 금지. |
| 이상탐지 정답 라벨 부재 | 합성 주입 평가. 검증 전 성능 미주장. |
| 비용 폭탄 | spot + TTL + 유휴 삭제(가이드 §8). |
| Binance API rate limit/약관 | 공개 스트림만, 백오프·캐시. 매매 미수행. |

---

## 11. 이 프로젝트가 입증하는 역량
- **운영 정합성**: offset/체크포인트 불일치 장애를 브로커 수명 분리 + 멱등 MERGE로 정식 해결하고, 재처리 멱등성을 검증 잡으로 증명.
- **오케스트레이션**: 수동 백필을 REST gap-fill + Airflow KubernetesExecutor 자동 백필로 전환.
- **k8s 운영**: Kafka·Spark·Airflow·모델 학습 Job을 k8s에서 운영.
- **end-to-end + MLOps**: 수집→정제→적재→이상탐지→재학습까지 산업 AIoT형 파이프라인을 축소판으로 구현.
- **스트리밍 이상탐지**: 이상탐지를 실제 운영 파이프라인 위에서 동작시켜 데이터·실무를 연결.

---

## 12. 다음 액션
1. PRD 범위 확정(1차 목표 — 권장: **P2 필수, P4까지 도전**).
2. 표준 k8s 학습 Job 패턴(Dockerfile/Job/PVC/TTL)을 레포 구조로 이식.
3. P0 WebSocket ingestor부터 착수(가장 빠른 "실시간" 데모).
4. 각 Phase 완료 시 README에 데모 근거(재처리 멱등성 증명·자동 백필 로그·이상탐지 결과)를 기록.
