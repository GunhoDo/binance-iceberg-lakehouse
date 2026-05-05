# Binance Iceberg Lakehouse MVP

Binance 공개 시장 데이터(`trades`, `klines`)와 시뮬레이션 주문 이벤트(`orders`)를 이용해
Apache Iceberg 기반 Lakehouse를 구축하는 데이터 엔지니어링 MVP.

## 무엇을 하는 프로젝트인가

거래소 도메인의 세 가지 이벤트(체결, 캔들, 주문)를 이벤트 성격별로 Kafka topic과
Iceberg table로 분리해 수집·정제·갱신·집계하고, Iceberg metadata를 기반으로 운영 지표를
관찰하는 파이프라인이다.

본 프로젝트는 거래 시스템이 아니라 데이터 플랫폼 MVP다.
실제 매매·자동매매·전략 추천은 Non-Goal로 명시한다 (`docs/PRD.md` §5).

## Layer 명명

본 프로젝트는 Bronze/Silver/Gold 대신 **Raw / Processed / Serving** 명칭을 사용한다.

| 본 프로젝트 명명 | 메달리온 대응 |
|---|---|
| Raw | Bronze |
| Processed | Silver |
| Serving | Gold |

이유: layer의 책임이 이름에서 바로 드러나도록 하기 위함이다.
자세한 결정 근거는 `docs/decisions.md` 참조.

## 데이터 출처와 시뮬레이션 정책

| Event | Source | 비고 |
|---|---|---|
| `trades` | Binance public market data | 실데이터 |
| `klines` | Binance public market data | 실데이터, interval 진행 중 반복 update |
| `orders` | simulator | user-level private data를 대체하는 합성 이벤트 |

거래소의 사용자별 주문 데이터는 본질적으로 private이므로 본 프로젝트는 실데이터인 척
가장하지 않고, 도메인 가정을 명시한 시뮬레이터로 대체한다 (`docs/simulator_design.md`).

## 디렉토리 구조

```
binance-iceberg-lakehouse/
├── README.md
├── docs/                 # PRD, decisions, architecture, simulator design
├── collectors/           # Binance public market data 수집기
├── simulators/           # orders simulator
├── streams/              # Kafka → Raw Zone Spark Structured Streaming
├── dags/                 # Airflow DAG
├── jobs/                 # processed/serving Spark batch job
├── sql/                  # Iceberg DDL, MERGE, metadata 조회 SQL
├── data/                 # samples, raw (gitignored 대상)
├── tests/
├── docker-compose.yml
└── requirements.txt
```

## Roadmap (요약)

| Phase | 내용 |
|---|---|
| 0 | Study & Design |
| 1 | Kafka + Raw Zone MVP |
| 2 | Iceberg Core MVP (processed_trades / klines / orders, MERGE, compaction) |
| 3 | Observability + Airflow |
| 4 | QuickSight Dashboard |
| 5 | Maintenance |

상세는 `docs/PRD.md` §17, `docs/roadmap.md`.

## 실행 안내

본 README의 실행 절차(docker-compose, Spark submit, Airflow trigger 등)는
각 Phase가 끝난 시점에 그 Phase에서 검증한 명령만 추가한다.
검증되지 않은 실행 절차는 적지 않는다.

## 참고 문서

- `docs/PRD.md` — 프로젝트 정의서
- `docs/decisions.md` — 설계 결정 기록과 보류한 결정
- `docs/architecture.md` — 아키텍처 다이어그램
- `docs/simulator_design.md` — 주문 시뮬레이터 설계
- `docs/roadmap.md` — Phase별 작업 항목
- `docs/operations.md` — 운영 지표 / Airflow / 임계값
- `docs/quicksight_metrics.md` — 대시보드 지표 정의
