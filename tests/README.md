# tests

Phase별로 다음 단위 테스트를 추가한다.

- Phase 1: simulator 출력 schema, collector 응답 파싱
- Phase 2: MERGE 결과의 키 단위 일관성, dedup 로직
- Phase 3: data_quality / table_health 측정 결과 schema

테스트 프레임워크는 Phase 1에서 결정한다.
