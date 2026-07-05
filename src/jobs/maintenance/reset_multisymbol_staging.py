"""reset_multisymbol_staging.py — staging_klines/staging_orders 초기화 (Phase K4)

멀티심볼 실데이터 E2E 를 처음부터 돌릴 때 필요한 재현성 보정 스크립트.

배경: staging_klines/staging_orders 는 Kafka (topic, partition, offset) 조합으로
멱등 MERGE(WHEN NOT MATCHED INSERT) 한다. 그런데 이 오프셋 공간은 한 Kafka
토픽·파티션의 "현재 수명" 안에서만 유일하다. k3d 를 재생성하거나 예전 단일심볼
데이터가 같은 오프셋 대역을 이미 점유한 상태에서 새 멀티심볼 메시지를 적재하면,
새 (ETH/SOL) 행이 옛 (BTC) 행과 오프셋이 겹쳐 "이미 존재"로 드롭된다(참고 D31).

따라서 새 멀티심볼 데이터셋으로 E2E 를 돌리기 전에 이 두 스테이징 테이블을 비운다.
스테이징은 raw 에서 언제든 재생성 가능한 중간 계층이고 Iceberg 스냅샷으로 되돌릴 수
있어 안전하다. processed/summary 는 비즈니스 키(klines: symbol/interval/open_time,
orders: order_id)로 MERGE 하므로 건드리지 않는다(멀티심볼이 자연히 합류).

읽기/쓰기 전용, 인자 없음. spark-submit 로 실행.
"""

from __future__ import annotations

from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import STAGING_KLINES, STAGING_ORDERS


def run() -> None:
    spark = get_spark("phase_k4_reset_multisymbol_staging")

    for table in (STAGING_KLINES, STAGING_ORDERS):
        before = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
        spark.sql(f"DELETE FROM {table}")
        after = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
        print(f"[reset] {table} before={before} after={after}")

    print("[phase_k4_reset_multisymbol_staging] complete")
    spark.stop()


if __name__ == "__main__":
    run()
