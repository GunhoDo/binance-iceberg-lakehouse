# Simulator Design — Orders

본 시뮬레이터는 **user-level private order data를 대체하는 합성 이벤트 생성기**다.
실제 Binance 주문이라고 주장하지 않는다.

## 왜 필요한가

거래소의 user-level 주문(NEW, FILL, CANCEL 등)은 public market data가 아니다.
Binance에서도 user data stream / account API 영역에 속하며 API key와 signature가
필요한 private 영역이다. 따라서 본 프로젝트는 이를 시뮬레이터로 대체하고, 그 사실을
명시한다.

## 시뮬레이터의 도메인 가정 (PRD §6.3)

| 항목 | 설계 방식 |
|---|---|
| 주문 도착률 | 시간 구간별 Poisson 또는 고정 rate 기반 |
| 주문 방향 | BUY / SELL 비율 config |
| 주문 가격 | 최근 kline close price 주변 분포에서 샘플링 |
| 주문 수량 | log-normal 또는 fixed range 기반 |
| 주문 상태 | NEW, PARTIALLY_FILLED, FILLED, CANCELED |
| 취소율 | config 기반 |
| 부분 체결률 | config 기반 |
| 체결 판단 | trade price 또는 kline close price와 주문 가격 비교 |

위 가정들은 **합성 데이터를 만들기 위한 명시적 가정**이며, 시장 행동을 정확히
재현하려는 목적이 아니다. 가정을 명시하는 이유는 평가자/독자가 "이 데이터가 어떻게
만들어졌는지" 즉시 확인할 수 있도록 하기 위해서다.

## 출력 이벤트 형식 (PRD §10.2)

`raw_orders` 적재 대상 컬럼:

- `order_id`
- `client_id`
- `symbol`
- `side` (BUY/SELL)
- `order_type`
- `order_price`
- `order_qty`
- `event_type`
- `order_status`
- `event_time`
- `simulated_parameters` (이 이벤트가 어떤 가정값으로 생성됐는지 추적용)
- `ingest_time`

`simulated_parameters`는 시뮬레이터 가정을 이벤트 단위로 추적하기 위한 컬럼이다.
JSON / map 형식으로 보존하며, 정확한 schema는 Phase 1에서 결정한다.

## 시뮬레이터가 하지 않는 것

- 실제 시장 행동 재현 (microstructure, queue position 등)
- order book 전체 재구성
- 매매 전략 / 수익률 시뮬레이션

위 항목들은 본 프로젝트의 Non-Goal이다 (PRD §5).

## 보류 / 미정

- 정확한 도착률 분포 파라미터, 가격 분포 파라미터, 수량 분포 파라미터 — config로
  분리해 Phase 1 구현 시점에 채운다.
- 부분 체결의 step 수와 시간 간격 — Phase 1에서 결정.

`decisions.md` D6, D9 참조.
