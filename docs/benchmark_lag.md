# 스트리밍 end-to-end lag 벤치마크 (P2/P3)

> PRD v2 센터피스. 통제 리플레이 부하 위에서 Kafka→Spark Structured Streaming→Iceberg의
> end-to-end lag를 측정(P2)하고, 튜닝 레버별 기여도를 분해(P3)한다.
> **정직 원칙**: 아래 수치는 모두 실측이며, baseline은 조작 없는 naive 기본값에서 출발한다.

## 1. 측정 정의

- **end-to-end lag** = `iceberg_commit_ts − produce_ts` (ms)
  - `produce_ts`: 리플레이 하니스(`infra/replay_harness.py`)가 각 메시지를 Kafka에 발행하는
    순간의 wall-clock. 리플레이 데이터의 원본 거래 시각(2024년)은 lag 기준으로 무의미하므로
    **주입 시점**을 기준점으로 삼는다.
  - `iceberg_commit_ts`: Spark `foreachBatch`가 배치를 로컬 Iceberg에 append(커밋)한 직후 시각.
  - 리플레이와 계측 잡을 **같은 컨테이너(같은 시계)**에서 실행해 host↔container 시계 왜곡 제거.
- **p50/p95/p99**: 전체 30,000건 lag 분포의 백분위수.
- **throughput**: `samples / (max(commit_ts) − min(produce_ts))` — 백로그 드레인 실효 처리량.

## 2. 셋업

```
data.binance.vision 2024-01 BTCUSDT trades (실데이터)
   └─ infra/replay_harness.py  (초당 N건 고정, produce_ts 심음)   ── 통제 부하
        │
        ▼  Kafka(KRaft, docker) topic=trades
        ▼  src/bench/lag_stream.py  (Spark Structured Streaming)
   로컬 Iceberg (hadoop catalog, file://) ── AWS 불요
        │  foreachBatch: 커밋 직후 commit_ts 기록
        ▼  local.bench.lag_samples  (produce_ts, commit_ts, lag_ms, config_label)
        ▼  src/bench/lag_report.py → p50/p95/p99 + throughput 표
```

- 실행 환경: docker-compose (`kafka` + `spark-runner`). Spark 3.5.5, Iceberg 1.7.0, openjdk-17.
- **측정 모델 = inject-then-drain**: 토픽에 30,000건을 채운 뒤 스트림이 드레인한다. lag에는
  큐 대기 시간이 포함되며, 이는 "고정 백로그를 각 설정이 얼마나 빨리 커밋하는가"를 재는 것이다.

## 3. 결과 (실측)

동일 부하: **30,000건, 3,000 msg/s 주입**. 소비자 레버만 변경.

| config | 레버 | p50 | p95 | p99 | max | throughput |
|--------|------|-----|-----|-----|-----|-----------|
| baseline | 트리거 5s · 배치 5000 | 15.40s | **25.61s** | 26.01s | 26.11s | 868/s |
| +trigger | 트리거 **0s** · 배치 5000 | 9.27s | **12.89s** | 13.29s | 13.53s | 1,918/s |
| +batch | 0s · 배치 **30000** | 9.06s | 13.56s | 13.96s | 14.20s | 2,113/s |

### 레버별 기여도 분해

- **+trigger가 결정적**: p95 **25.61s → 12.89s (−49.7%)**, throughput **868 → 1,918/s (2.2배)**.
  baseline의 병목은 5초 트리거 대기였고, 이를 제거한 것이 개선의 거의 전부다.
- **+batch는 lag를 줄이지 못함(오히려 +0.67s), throughput만 +10%**: 30,000건을 한 배치로 읽으면
  먼저 도착한 레코드가 배치 전체 처리를 기다려 **tail latency가 개선되지 않는다.** 처리량은 향상.
- **결론**: tail lag는 **트리거 정책이 지배**한다. 배치 확대는 처리량 레버이지 lag 레버가 아니다.

### 헤드라인

> 동일 부하(30k, 3,000/s)에서 스트리밍 end-to-end **p95 lag 25.6s → 12.9s (약 50% 단축)**,
> **처리량 2.2배**. 레버별 기여도 분해로 tail latency가 트리거 정책에 지배됨을 규명.

## 4. 재현 절차

```bash
# 0) 인프라
docker compose -f infra/docker-compose.yml --profile streaming up -d
docker compose -f infra/docker-compose.yml --profile airflow up -d spark-runner
bash infra/download_data.sh   # data/raw/BTCUSDT-trades-2024-01.csv

# 1) config 1회분: 토픽 재생성 → 30k 주입 → earliest 드레인
#    (spark-runner 컨테이너 내부에서, JAVA_HOME=/usr/lib/jvm/java-17-openjdk-<arch>)
python infra/replay_harness.py --file data/raw/<trades>.csv \
    --bootstrap kafka:29092 --rate 3000 --limit 30000
python src/bench/lag_stream.py --label baseline --run-id run010 \
    --starting-offsets earliest --trigger '5 seconds' --max-offsets 5000 --duration 50
# +trigger: --trigger '0 seconds' --max-offsets 5000  (run-id/label 변경)
# +batch  : --trigger '0 seconds' --max-offsets 30000

# 2) 리포트
python src/bench/lag_report.py
```

## 5. 정직성 한계 (검증 전 미주장)

- **로컬 Iceberg 기준**: S3/Glue가 아니라 로컬 hadoop catalog에 커밋한다. 실 S3 커밋 지연은
  포함되지 않으며(=벤치가 더 깨끗), 프로덕션 S3 수치는 다를 수 있다. ablation 방법론은 동일.
- **inject-then-drain 모델**: lag에 큐 대기가 포함된다. steady-state(동시 주입+소비) 수치와
  다를 수 있으나, 고정 백로그 비교로는 공정하다.
- **병렬성 레버 제외**: topic 파티션이 1개라 이 append 파이프라인에서 shuffle 병렬성은
  효과가 없어 정직하게 제외했다. 파티션 확대는 별도 실험 대상.
- **환경 메모**: aarch64 openjdk-17에서 `percentile_approx` whole-stage codegen이 SIGSEGV를
  내는 사례가 있어 리포트 세션은 codegen을 끈다(`src/bench/lag_report.py`).
