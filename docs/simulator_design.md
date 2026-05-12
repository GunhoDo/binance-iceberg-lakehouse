# Simulator Design — Orders

본 시뮬레이터는 **user-level private order data를 대체하는 합성 주문 이벤트 생성기**다.  
이 시뮬레이터의 출력은 **실제 Binance 주문이 아니다**. 본 프로젝트에서는 Iceberg MERGE 기반 주문 상태 관리 실험을 위해 synthetic order events를 생성한다.

## 왜 필요한가

거래소의 user-level 주문 이벤트(`NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`)는 public market data가 아니다.  
Binance에서도 user data stream / account API 영역에 속하며, API key와 signature가 필요한 private 영역이다.

따라서 본 프로젝트는 실제 private order data를 사용하지 않고, 명시적인 도메인 가정을 가진 시뮬레이터로 주문 이벤트를 생성한다. 이 데이터는 실제 시장 행동을 재현하기 위한 것이 아니라, 다음 실험을 위한 것이다.

- Kafka `orders` topic 발행
- Raw Zone append-only 적재
- `staging_orders` 정제 적재
- `processed_orders`에서 `order_id` 기준 최신 상태 MERGE
- MOR 기반 row-level update 실험

## 현재 시뮬레이터의 도메인 가정

| 항목 | 현재 구현 |
|---|---|
| 기본 symbol | `BTCUSDT` |
| client 수 | `C0001` ~ `C0050` |
| 주문 도착 간격 | `0 ~ max_sleep_ms` 사이 random sleep |
| 주문 방향 | `BUY` / `SELL` 랜덤 선택 |
| 주문 타입 | `LIMIT` |
| 기준 가격 | 실행 인자 `--reference-close` |
| 주문 가격 | `reference_close ± 0.3%` 범위에서 uniform sampling |
| 주문 수량 | `0.001 ~ 0.08` 범위에서 uniform sampling |
| 부분 체결률 | `0.45` |
| 취소율 | `0.25` |
| 이벤트 시간 | 기본은 현재 시각, `--start-ts`/`--end-ts` 지정 시 해당 기간 안에서 random sampling |
| 주문 ID namespace | 기본은 `O00000001` 형식, 월별 실행 시 `--order-id-prefix` 권장 |
| Kafka topic | 기본값 `orders` |
| Kafka message key | `order_id` |

현재 구현은 빠른 Phase 2 검증을 위한 단순 모델이다.  
향후에는 주문 도착률을 Poisson/exponential 분포로 확장하거나, 가격/수량 분포를 config로 분리할 수 있다.

## 기간 기반 생성 정책

Phase 3 window 기반 pipeline은 `start_ts <= event_time < end_ts` 조건으로 데이터를 처리한다. 따라서 2024년 2월 market data와 함께 2월 주문 KPI를 보려면 orders simulator도 같은 기간으로 실행해야 한다.

`--start-ts`와 `--end-ts`가 둘 다 제공되면 simulator는 각 주문의 `NEW` 이벤트 기준 시각인 `base_time_ms`를 다음 범위에서 random sampling한다.

```text
[start_ms, end_ms - MAX_LIFECYCLE_OFFSET_MS)
```

현재 `MAX_LIFECYCLE_OFFSET_MS`는 `15_000` milliseconds다. `PARTIALLY_FILLED`, `FILLED`, `CANCELED` 이벤트는 `NEW` 이후 최대 15초 안에서 생성되므로, 이 여유 구간을 빼고 sampling해야 최종 이벤트가 `end_ts`를 넘지 않는다.

`--start-ts` 또는 `--end-ts` 중 하나만 제공하면 오류로 종료한다. 또한 `end_ms - start_ms <= MAX_LIFECYCLE_OFFSET_MS`인 너무 짧은 기간도 오류로 처리한다.

기간 인자를 제공하지 않으면 기존처럼 현재 시각 기준으로 주문 이벤트를 생성한다. 이 모드는 빠른 로컬 smoke test용이며, 월별 backfill이나 dashboard KPI 검증에는 기간 기반 실행을 사용한다.

## 주문 ID namespace 정책

`processed_orders`는 `order_id` 기준으로 최신 주문 상태를 MERGE한다. 따라서 월별로 simulator를 다시 실행할 때 `order_id`가 같으면 서로 다른 달의 주문이 같은 주문의 상태 update처럼 처리될 수 있다.

이를 피하기 위해 월별 실행에서는 `--order-id-prefix`를 사용한다.

```text
prefix 202401, index 1 -> O20240100000001
prefix 202402, index 1 -> O20240200000001
```

`--order-id-prefix` 기본값은 빈 문자열이다. 즉 별도 prefix를 주지 않으면 하위 호환을 위해 기존 형식인 `O00000001`부터 생성된다. 운영성 있는 월별 적재나 재처리에서는 `YYYYMM` 형식 prefix 사용을 권장한다.

## 주문 상태 전이

현재 시뮬레이터는 하나의 주문에 대해 다음 상태 전이 중 하나를 생성한다.

```text
NEW → FILLED
```

```text
NEW → CANCELED
```

```text
NEW → PARTIALLY_FILLED → FILLED
```

```text
NEW → PARTIALLY_FILLED → CANCELED
```

각 주문은 항상 `NEW` 이벤트로 시작한다.  
이후 `partial_fill_rate`에 따라 `PARTIALLY_FILLED` 이벤트가 추가될 수 있고, 최종 상태는 `FILLED` 또는 `CANCELED`로 결정된다.

## 출력 이벤트 형식

Kafka `orders` topic으로 발행되는 `message_value`는 JSON 형식이다.

주요 필드는 다음과 같다.

| 필드 | 설명 |
|---|---|
| `order_id` | 주문 ID. 예: `O00000001`, 월별 prefix 사용 시 `O20240200000001` |
| `client_id` | 합성 client ID. 예: `C0027` |
| `symbol` | 거래 symbol. 기본값 `BTCUSDT` |
| `side` | `BUY` 또는 `SELL` |
| `order_type` | 현재는 `LIMIT` |
| `order_price` | 주문 가격. decimal string |
| `order_qty` | 주문 수량. decimal string |
| `filled_qty` | 해당 이벤트 시점의 누적 체결 수량 |
| `avg_fill_price` | 평균 체결 가격 |
| `event_type` | `ORDER_NEW`, `ORDER_PARTIALLY_FILLED`, `ORDER_FILLED`, `ORDER_CANCELED` |
| `order_status` | `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED` |
| `event_time` | UTC epoch milliseconds 문자열 |
| `simulated_parameters` | 이벤트 생성에 사용된 시뮬레이션 파라미터 |

예시:

```json
{
  "order_id": "O00000001",
  "client_id": "C0027",
  "symbol": "BTCUSDT",
  "side": "SELL",
  "order_type": "LIMIT",
  "order_price": "43128.51273461",
  "order_qty": "0.05838400",
  "filled_qty": "0.05838400",
  "avg_fill_price": "43128.51273461",
  "event_type": "ORDER_FILLED",
  "order_status": "FILLED",
  "event_time": "1778160000000",
  "simulated_parameters": {
    "partial_fill_rate": 0.45,
    "cancel_rate": 0.25,
    "price_deviation_rate": 0.003,
    "reference_close": 43000.0,
    "qty_range": [0.001, 0.08],
    "simulation_start_ts": "2024-02-01T00:00:00",
    "simulation_end_ts": "2024-03-01T00:00:00",
    "order_id_prefix": "202402",
    "max_lifecycle_offset_ms": 15000,
    "note": "synthetic order event, not real Binance private data"
  }
}
```

## `simulated_parameters` 저장 방식

`simulated_parameters`는 이벤트가 어떤 가정값으로 생성됐는지 추적하기 위한 메타데이터다.

현재 Phase 2에서는 `staging_orders`와 `processed_orders`에서 이 값을 구조화된 `MAP`이나 `STRUCT`로 강제 파싱하지 않고, **JSON string**으로 보존한다.

이유는 다음과 같다.

- 내부 값에 숫자, 배열, 문자열이 함께 존재한다.
- Iceberg/Athena 호환성을 단순하게 유지할 수 있다.
- 현재 Phase 2에서는 분석 대상이라기보다 생성 조건 추적용 메타데이터다.

향후 simulator parameter 분석이 중요해지면 별도 schema를 정의하거나 config table로 분리한다.

## 실행 방법

```bash
python src/simulators/orders_simulator.py \
  --bootstrap localhost:9092 \
  --topic orders \
  --num-orders 1000 \
  --reference-close 43000
```

월별 backfill / dashboard KPI 검증용 실행 예시:

```bash
python src/simulators/orders_simulator.py \
  --bootstrap localhost:9092 \
  --topic orders \
  --num-orders 1000 \
  --reference-close 43000 \
  --symbol BTCUSDT \
  --start-ts 2024-02-01T00:00:00 \
  --end-ts 2024-03-01T00:00:00 \
  --order-id-prefix 202402 \
  --seed 42
```

주요 인자:

| 인자 | 기본값 | 설명 |
|---|---:|---|
| `--bootstrap` | `localhost:9092` | Kafka bootstrap server |
| `--topic` | `orders` | 발행 대상 Kafka topic |
| `--num-orders` | `1000` | 생성할 주문 수 |
| `--reference-close` | `43000.0` | 주문 가격 sampling 기준 가격 |
| `--symbol` | `BTCUSDT` | 주문 symbol |
| `--max-sleep-ms` | `10` | 주문 간 최대 sleep milliseconds |
| `--start-ts` | 없음 | 주문 이벤트를 생성할 기간의 시작 timestamp. `--end-ts`와 함께 사용 |
| `--end-ts` | 없음 | 주문 이벤트를 생성할 기간의 exclusive 종료 timestamp. `--start-ts`와 함께 사용 |
| `--order-id-prefix` | `""` | `order_id` 충돌 방지를 위한 namespace prefix. 월별 실행 시 `YYYYMM` 권장 |
| `--seed` | 없음 | 동일 입력으로 재현 가능한 synthetic data 생성을 위한 random seed |

예를 들어 2월 market data만 적재한다면 trades/klines만 2월 기준으로 넣어도 된다. 하지만 2월 `order_execution_summary`까지 dashboard에서 보려면 orders simulator도 2월 기간과 2월 prefix로 다시 생성해야 한다.

## 시뮬레이터가 하지 않는 것

본 시뮬레이터는 다음을 목표로 하지 않는다.

- 실제 시장 microstructure 재현
- order book 전체 재구성
- queue position 모델링
- 매매 전략 / 수익률 시뮬레이션
- 실제 Binance private order data 복제

위 항목들은 본 프로젝트의 Non-Goal이다.

## Lakehouse 처리 흐름

생성된 order event는 다음 흐름으로 처리된다.

```text
orders_simulator.py
   ↓
Kafka topic: orders
   ↓
raw_orders
   ↓
staging_orders
   ↓
processed_orders
```

- `raw_orders`: Kafka 원본 이벤트를 append-only로 보관한다.
- `staging_orders`: JSON을 파싱하고 타입을 정규화한 이벤트 로그다.
- `processed_orders`: `order_id` 기준 최신 주문 상태만 유지하는 Iceberg table이다.

`processed_orders`는 주문 상태 전이가 발생하는 mutable table이므로 MOR(Merge-on-Read) 설계 대상이다.

`order_execution_summary`는 `processed_orders.updated_at`을 시간축으로 사용한다. 따라서 market data의 월과 simulated orders의 `event_time` 월이 다르면 같은 dashboard 기간에서 시장 KPI와 주문 KPI가 서로 어긋난다.

## 보류 / 확장 예정

- 주문 도착률을 Poisson/exponential 분포로 확장
- 가격 분포와 수량 분포를 config 파일로 분리
- `reference_close`를 고정 인자가 아니라 최근 kline close에서 동적으로 가져오기
- 부분 체결 step 수와 시간 간격을 config화
- simulator run 단위 추적을 위한 `run_id` 또는 `batch_id` 추가
- `simulated_parameters`를 별도 config table로 분리
