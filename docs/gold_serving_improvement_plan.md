# Serving 레이어 개선 전략 — VWAP 벤치마크 & 실행 성과(슬리피지) 지표

> 대상: `market_hourly_summary`, `order_execution_summary` (Serving/Gold 레이어)
> 상태: **확정 (2026-07-04)** — `docs/ROADMAP.md` Phase G(=I1)·Phase X(=I2/I3)로 채택. 트리거·롤아웃 단계는 §7 참조.
> 정본 참조: `docs/PRD.md`(v3), `docs/ROADMAP.md`, `docs/decisions.md`.
>
> ⚠️ **순서 제약(확정)**: I2(슬리피지)는 시뮬레이터 **실시장 앵커링(ROADMAP Phase A) 완료 후에만** 착수한다.
> 현 시뮬레이터는 `--reference-close` 고정값 주변에서 주문가를 뿌리므로, 앵커링 없이 슬리피지를 계산하면
> "고정 기준값 vs 실시장의 괴리"만 측정하는 무의미한 지표가 된다. I1(VWAP)은 실 체결 데이터만 쓰므로 즉시 가능.

---

## 1. 배경 & 문제 정의

현재 Serving 레이어는 두 지표군을 서빙한다.

- `market_hourly_summary` — 시황: OHLC + kline/trade 파생 지표 + maker/taker 카운트, 그리고 `avg_trade_price`.
- `order_execution_summary` — 주문 실행: `fill_rate`, `cancel_rate`, `avg_fill_delay_sec` 등.

두 테이블은 각각 "시장이 어떻게 움직였나"와 "주문이 처리됐나"에 답한다. 그러나 **둘이 서로를 참조하지 않는다.** 그 결과 지금 구조로는 다음 질문에 답할 수 없다.

> "우리 주문이 **그 시점 시장 가격 대비** 얼마나 유리/불리하게 체결됐는가?"

이 질문이 트레이딩 데스크가 실제로 가장 중요하게 보는 **실행 성과(execution performance)** 이며, 답하려면 두 가지가 필요하다.

1. 시점별 **시장 기준가(벤치마크)** — 단순 종가/평균가가 아니라 거래량 가중 평균가(VWAP).
2. 체결가를 그 벤치마크와 비교한 **슬리피지(slippage) 지표**.

현재 `market_hourly_summary`에는 `avg_trade_price`는 있지만 **VWAP은 없고**, `order_execution_summary`에는 체결가·슬리피지 개념 자체가 없다. 본 문서는 이 두 공백을 메우는 개선안을 제시한다.

---

## 2. 개선안 개요

| # | 개선안 | 대상 테이블 | 핵심 가치 |
|---|---|---|---|
| I1 | VWAP 벤치마크 컬럼 추가 | `market_hourly_summary` | 거래 무게중심가 = 산업 표준 기준선 확보 |
| I2 | 실행 성과(슬리피지) 지표 추가 | `order_execution_summary` | 체결가를 벤치마크와 비교 → 실행 지표 체계 확보 (합성 단계에선 분포 통계 — §4.1.2) |
| I3 | 두 지표를 잇는 서빙 뷰/조인 표준화 | 신규 뷰 or 조인 규약 | 시황 ↔ 실행을 한 화면에서 cross-reference |

핵심 아이디어는 **가격 형성(시세) 지표와 주문 실행 지표를 하나의 벤치마크(VWAP)로 연결**하는 것이다. 시세 집계 로직과 주문 집계 로직은 이미 각각 존재하므로, 신규 파이프라인을 만들기보다 **기존 두 요약 테이블에 컬럼을 얹고 그 둘을 조인**하는 최소 침습 방식으로 접근한다.

---

## 3. 개선안 I1 — VWAP 벤치마크

### 3.1 정의

시간버킷·심볼 단위 거래량 가중 평균가.

```
VWAP = Σ(price × qty) / Σ(qty)
```

`processed_trades`는 `quote_qty`(= price × qty)를 이미 보유하므로 계산은 아래로 단순해진다.

```
VWAP = Σ(quote_qty) / Σ(qty)
```

### 3.2 DDL 변경 (`market_hourly_summary`)

`avg_trade_price` 옆에 벤치마크 컬럼을 추가한다. (기존 컬럼은 유지 — 하위 호환)

```sql
ALTER TABLE glue.binance_lakehouse.market_hourly_summary
    ADD COLUMN vwap DECIMAL(20, 8) AFTER avg_trade_price;
```

`avg_trade_price`(단순 산술 평균)와 `vwap`(거래량 가중)을 나란히 두면, 거래가 특정 가격대에 몰렸는지(둘의 괴리)를 바로 읽을 수 있다.

### 3.3 집계 로직 (기존 trade_hourly CTE에 한 줄 추가)

`src/jobs/daily/06_build_market_hourly_summary_window.py`의 `trade_hourly` 집계에 추가:

```sql
CAST(
    SUM(t.quote_qty) / NULLIF(SUM(t.qty), 0)
    AS DECIMAL(20, 8)
) AS vwap
```

`NULLIF`로 분모 0(거래 없음)을 방어한다. MERGE의 `WHEN MATCHED ... UPDATE SET` 및 최종 SELECT 절에 `vwap` 매핑을 추가하면 된다. 쓰기 패턴(윈도우 증분 MERGE, MOR)은 그대로 유지된다.

---

## 4. 개선안 I2 — 실행 성과(슬리피지)

### 4.1 정의

체결가가 그 시점 시장 벤치마크(VWAP) 대비 얼마나 유리/불리했는지를 **bps(basis point, 0.01%)** 로 측정한다. 매수/매도 방향을 부호에 반영해, **양수 = 유리(더 싸게 사거나 비싸게 팜), 음수 = 불리** 로 통일한다.

```
BUY  slippage_bps =  (vwap - avg_fill_price) / vwap × 10000
SELL slippage_bps =  (avg_fill_price - vwap) / vwap × 10000
```

`processed_orders`에는 이미 `avg_fill_price`, `side`가 있으므로 추가 원천 데이터는 필요 없다. 벤치마크만 `market_hourly_summary`에서 조인하면 된다.

### 4.1.1 앵커 기준점 ≠ 벤치마크 기준점 (순환 방지 원칙 — 필수)

시뮬레이터 앵커링(ROADMAP Phase A)과 슬리피지 벤치마크는 **서로 다른 가격 시계열**을 써야 한다.

- **앵커 기준점** = 주문 생성 시점 T의 **의사결정 시점 가격** — minute-bucket **close** (실거래라면 mid/last에 해당).
- **벤치마크 기준점** = 시간 **구간** 전체의 **VWAP** (`market_hourly_summary.vwap`).

만약 주문을 구간 VWAP 자체에 앵커링해 놓고 같은 VWAP으로 슬리피지를 재면, 기대 슬리피지 ≈ 0이 되고 남는 것은 시뮬레이터 산포 노이즈의 분산뿐이다 — **자기 파라미터를 되읽는 순환(circularity)** 이며 지표가 무의미해진다. 반대로 "시점 close에 앵커 → 구간 VWAP으로 벤치마크"로 분리하면, 슬리피지는 구간 내 가격 경로와 주문 타이밍 분포의 관계를 반영하는 유효한 통계가 된다.

이 원칙은 앵커링 구현 변경 시에도 불변 조건이다: **앵커 소스가 벤치마크 소스와 같아지는 순간 I2는 무효.** 검증은 §7 R1.5·R2 완료 기준에 포함한다.

### 4.1.2 정직한 포지셔닝 — 앵커링은 필요조건이지 충분조건이 아니다

앵커링을 해도 주문은 여전히 **무작위 산포 합성 주문**이다. 따라서 이 단계의 슬리피지는 트레이더의 "실행 스킬"이 아니라 **분포 통계**(앵커된 무작위 주문이 구간 VWAP 대비 어떻게 분포하는가)다. 이 단계 슬리피지의 정당한 용도는 두 가지뿐이다:

1. **파이프라인·지표의 기계적 검증** — 조인·부호 규약·NULL 전파·멱등성이 올바른지.
2. **미래 실전략용 인프라** — 실제 주문(또는 전략 시뮬레이션)이 들어오는 순간 그대로 쓸 수 있는 벤치마크·지표 체계 선구축.

"전략 성과 정량화"는 실주문/전략 데이터가 연결된 이후에만 주장할 수 있다. 데모·문서에서 이 경계를 넘는 표현은 금지.

### 4.2 DDL 변경 (`order_execution_summary`)

**방향 분리 확정**: 시간 버킷 안에 BUY/SELL이 섞이므로 혼합 평균 한 컬럼은 부호가 상쇄되어 정보를 잃는다. side별 컬럼으로 분리한다.

```sql
ALTER TABLE glue.binance_lakehouse.order_execution_summary
    ADD COLUMNS (
        benchmark_vwap        DECIMAL(20, 8),   -- 조인해 온 시장 기준가
        avg_buy_fill_price    DECIMAL(20, 8),   -- BUY 체결 주문 가중 평균 체결가
        avg_sell_fill_price   DECIMAL(20, 8),   -- SELL 체결 주문 가중 평균 체결가
        buy_slippage_bps      DOUBLE,           -- (vwap - buy_fill)/vwap × 10000, 양수=유리
        sell_slippage_bps     DOUBLE,           -- (sell_fill - vwap)/vwap × 10000, 양수=유리
        slippage_cost_quote   DECIMAL(30, 8)    -- 슬리피지의 금액 환산 (Σ filled_qty × 가격차)
    );
```

### 4.3 집계 로직 (`07_build_order_execution_summary_window.py` 확장)

`order_hourly` 집계에 체결가·체결 방향 기반 슬리피지 원자료를 추가하고, `market_hourly_summary`를 `(summary_hour, symbol)`로 LEFT JOIN 해 벤치마크를 붙인다.

```sql
-- order_hourly CTE 내부: side별 체결 주문의 가중 평균 체결가
CAST(
    SUM(CASE WHEN p.order_status = 'FILLED' AND p.side = 'BUY'
             THEN p.avg_fill_price * p.filled_qty END)
    / NULLIF(SUM(CASE WHEN p.order_status = 'FILLED' AND p.side = 'BUY'
                      THEN p.filled_qty END), 0)
    AS DECIMAL(20, 8)
) AS avg_buy_fill_price,
-- SELL도 동일 패턴으로 avg_sell_fill_price
```

```sql
-- 최종 SELECT: 벤치마크 조인 후 side별 슬리피지 계산
LEFT JOIN glue.binance_lakehouse.market_hourly_summary m
       ON m.summary_hour = keys.summary_hour
      AND m.symbol       = keys.symbol
...
m.vwap AS benchmark_vwap,
o.avg_buy_fill_price,
o.avg_sell_fill_price,
CAST((m.vwap - o.avg_buy_fill_price)  / NULLIF(m.vwap, 0) * 10000 AS DOUBLE) AS buy_slippage_bps,
CAST((o.avg_sell_fill_price - m.vwap) / NULLIF(m.vwap, 0) * 10000 AS DOUBLE) AS sell_slippage_bps
```

> 방향 분리는 확정 결정이다(§4.2). 순포지션 정규화 방식은 부호 상쇄로 정보를 잃어 비채택.

### 4.4 의존성 순서

슬리피지는 `market_hourly_summary.vwap`에 의존하므로, DAG에서 **market summary → order summary 순서**를 보장해야 한다. 현재 `06_...` → `07_...` 순서와 일치하므로 순서 변경은 불필요하고, `07` 잡의 upstream에 `06`을 명시(의존성 엣지 추가)만 하면 된다.

---

## 5. 개선안 I3 — 시황 ↔ 실행 연결 서빙 뷰

두 요약을 매번 조인하지 않도록, Grafana가 바로 읽을 표준 뷰를 둔다.

```sql
CREATE VIEW glue.binance_lakehouse.execution_vs_market AS
SELECT
    o.summary_hour,
    o.symbol,
    m.vwap                AS market_vwap,
    o.avg_buy_fill_price,
    o.avg_sell_fill_price,
    o.buy_slippage_bps,
    o.sell_slippage_bps,
    o.slippage_cost_quote,
    o.fill_rate,
    o.cancel_rate,
    o.avg_fill_delay_sec,
    m.high_price, m.low_price, m.kline_quote_volume
FROM glue.binance_lakehouse.order_execution_summary o
LEFT JOIN glue.binance_lakehouse.market_hourly_summary m
       ON m.summary_hour = o.summary_hour AND m.symbol = o.symbol;
```

이 뷰 하나로 "시장이 이렇게 움직인 시간대에, 우리 주문은 이만큼 체결됐고, 벤치마크 대비 이만큼 유불리했다"를 한 행에서 본다.

---

## 6. 기대 비즈니스 가치

- **실행 지표 체계의 선구축** — "체결은 됐지만 좋은 가격이었나"를 bps로 감시할 수 있는 인프라. 실주문/전략이 연결되면 슬리피지 악화가 곧 전략 튜닝·주문 로직 점검 트리거가 된다. (합성 단계의 경계는 §4.1.2)
- **표준 벤치마크 확보** — VWAP은 체결 성과 평가의 사실상 산업 표준. 이후 어떤 실행 지표든 이 기준선 위에서 해석 가능해진다.
- **비용의 가시화** — `slippage_cost_quote`로 슬리피지를 추상 지표가 아닌 **금액**으로 환산해 리스크·손익 관점에서 볼 수 있다.
- **시황과 실행의 결합 인사이트** — 변동성(high-low)·유동성(quote_volume)이 큰 시간대에 슬리피지가 어떻게 반응하는지 cross-reference 가능.
- **최소 침습** — 신규 원천/파이프라인 없이 기존 두 요약 테이블에 컬럼·조인만 추가. 증분 MERGE·MOR 설계는 그대로 유지.

> ⚠️ **정직한 caveat**: 위 가치 중 "성과 감시"는 실주문/전략 데이터가 연결된 이후의 이야기다. 앵커링된 합성 주문 단계에서 슬리피지는 실행 스킬이 아니라 **분포 통계**이며, 정당한 용도는 (1) 파이프라인·지표의 기계적 검증, (2) 미래 실전략용 인프라 선구축 두 가지다(§4.1.2).

---

## 7. 롤아웃 단계 & 트리거

| 단계 | 내용 | 트리거 / 완료 기준 |
|---|---|---|
| R1 | I1(VWAP) DDL + 집계 반영, 백필 (= ROADMAP **Phase G**) | `vwap`와 `avg_trade_price` 괴리 sanity check 통과 |
| R1.5 | **시뮬레이터 실시장 앵커링** (= ROADMAP **Phase A**) — I2의 전제 조건 | 같은 seed 바이트 동일 재현 + 주문 분포와 실 volume 상관 확인 + **앵커 소스가 minute close(구간 VWAP 아님)임을 확인 (§4.1.1)** |
| R2 | I2 슬리피지 — side 분리(확정) 집계 반영, 앵커링된 주문으로 백필 | 임의 시간대 수기 계산과 값 일치 + **순환 부재 검증: 슬리피지 분포가 0 주변 퇴화가 아니고 구간 변동성과 상관을 가짐 (§4.1.1)** |
| R3 | I3 서빙 뷰(Athena) + Grafana 패널(슬리피지 추세·비용) | 대시보드에서 이상 시간대 식별 가능 |
| R4 | 슬리피지 알람 규칙 (예: bps 임계 초과) | 실행 성과 저하 시 자동 인지 |

> R1.5~R4가 ROADMAP **Phase X**의 범위다. DAG 의존성 엣지(`build_market_hourly_summary >> build_order_execution_summary`) 추가도 R2에 포함(§4.4).

### 검증 관점

- VWAP 분모 0 방어(`NULLIF`) — 거래 공백 시간대에서 NULL 처리 확인.
- 방향 부호 규약 일관성 — 양수=유리 정의가 매수/매도 모두에서 성립하는지 단위 검증.
- 조인 커버리지 — `order_execution_summary`에 매칭되는 `market_hourly_summary` 행이 없을 때(벤치마크 결측) NULL 흐름 점검.
- 멱등성 — 같은 윈도우 재실행 시 슬리피지 값이 동일하게 재현되는지(MERGE 재실행 테스트).
