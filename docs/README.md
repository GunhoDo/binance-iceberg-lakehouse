# docs 인덱스

이 폴더는 **현재(v3) 방향** + 계속 이어지는 결정 로그를 담는다. v2 및 그 이전의 스펙·as-built 문서(architecture / operations / benchmark_lag / data_quality / grafana_metrics / simulator_design / deep-interview / 100x scale-out)는 **`prd-v2` 태그에 프리즈**돼 있으며, 아래로 복구·열람한다.

```bash
git show prd-v2:docs/benchmark_lag.md      # v2 lag 벤치마크 실측(p95 25.6s→12.9s)
git checkout prd-v2 -- docs/architecture.md  # 필요 시 개별 복구
```

## 현재 문서

| 문서 | 역할 |
|---|---|
| [PRD.md](PRD.md) | **정본 스펙 (v3)** — Gold 실행성과 서빙 + k3d 멀티심볼. 목표/Non-Goal/FR/NFR/마일스톤 |
| [ROADMAP.md](ROADMAP.md) | **실행 계획 (정본)** — Phase G/A/X/K 상세 작업·완료 기준, 보류(ML/Flink) 요약 |
| [gold_serving_improvement_plan.md](gold_serving_improvement_plan.md) | **설계 상세** — VWAP·슬리피지 DDL·집계 SQL, 순환 방지(§4.1.1)·정직 포지셔닝(§4.1.2) |
| [decisions.md](decisions.md) | **결정 로그 (계속 이어짐)** — v1~v2 설계 결정·보류 항목 기록, v3 결정도 여기 누적 |

## 버전 계보

| 버전 | 태그 | 헤드라인 | 상태 |
|---|---|---|---|
| v1 | `prd-v1` | 원본 배치 lakehouse | 프리즈 |
| v2 | `prd-v2` | 실시간 수집 + 스트리밍 lag 최적화 벤치마크 (P0~P5) | **delivered**, 프리즈 |
| v3 | (현재) | Gold 실행성과 서빙 + k3d 멀티심볼 운영화 | 진행 |
