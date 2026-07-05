"""spark_on_k8s.py — Airflow 태스크 → Spark-on-k8s 잡 실행 팩토리 (Phase K3)

Compose 시절 태스크는 `docker exec spark-runner run_job.sh`(로컬 master)였다. K3 는
Airflow 를 k8s(KubernetesExecutor)로 올리고, 각 Spark 잡을 KubernetesPodOperator 로
실행한다. KPO 파드가 곧 spark-submit(client 모드) 드라이버이며, 드라이버가 executor
파드를 직접 스케줄한다 — K2(infra/k8s/40-spark-job.template.yaml)의 실행 방식을 그대로
Airflow 태스크로 옮긴 것.

레이어: KubernetesExecutor 워커 파드 → (KPO) Spark 드라이버 파드 → executor 파드.
자격은 K2 와 동일하게 aws-creds Secret(드라이버 envFrom, executor secretKeyRef).
SA 는 K2 의 `spark`(executor 파드 생성 RBAC 보유).

k3d 자원 절약을 위해 드라이버/executor 1g, executor 1개로 낮춘다(Compose 는 2g).
"""

from __future__ import annotations

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


NAMESPACE = "binance-lakehouse"
SPARK_IMAGE = "binance-spark-k8s:latest"
SPARK_SA = "spark"
AWS_SECRET = "aws-creds"

# Airflow Jinja 매크로 — arguments 는 렌더링된다.
START_TS = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}"
END_TS = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}"
RUN_ID = "{{ run_id }}"


def _spark_submit_script(
    task_id: str, job_path: str, run_id: str, extra_env: str = ""
) -> str:
    """파드 안에서 실행할 spark-submit(client 모드) bash 스크립트.

    K2 템플릿과 동일 confs. podNamePrefix 는 지정하지 않는다 — Spark 가 appName 에서
    DNS 안전 이름을 파생하므로 Airflow run_id(특수문자 포함)를 그대로 쓰지 않는다.
    """
    return f"""
set -euo pipefail
{extra_env}
echo "driver pod ip=$POD_IP job={job_path} run={run_id}"
exec /opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode client \
  --name spark-{task_id} \
  --conf spark.kubernetes.namespace={NAMESPACE} \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SA} \
  --conf spark.kubernetes.container.image={SPARK_IMAGE} \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.executor.request.cores=1 \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID={AWS_SECRET}:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY={AWS_SECRET}:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_REGION={AWS_SECRET}:AWS_REGION \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_DEFAULT_REGION={AWS_SECRET}:AWS_DEFAULT_REGION \
  --conf spark.driver.host=$POD_IP \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.driver.maxResultSize=512m \
  --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby-$$ \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue.warehouse=s3://binance-iceberg-lake/warehouse \
  --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf spark.sql.shuffle.partitions=4 \
  /workspace/{job_path} \
  --start-ts "{START_TS}" \
  --end-ts "{END_TS}" \
  --run-id "{run_id}"
""".strip()


def spark_k8s_task(
    task_id: str,
    job_path: str,
    run_id_suffix: str = "",
    table_health_mode: str | None = None,
    do_xcom_push: bool = False,
) -> KubernetesPodOperator:
    """Spark 잡 1건을 KubernetesPodOperator(=드라이버 파드)로 실행하는 태스크.

    do_xcom_push=True 면 KPO 가 /airflow/xcom 을 마운트하고 xcom 사이드카를 붙인다 →
    잡(예: detect_gaps.py)이 /airflow/xcom/return.json 을 쓰면 XCom 으로 발행된다.
    """
    run_id = f"{RUN_ID}{run_id_suffix}"
    extra_env = (
        f'export TABLE_HEALTH_MODE={table_health_mode}\n'
        if table_health_mode is not None
        else ""
    )
    script = _spark_submit_script(task_id, job_path, run_id, extra_env)

    return KubernetesPodOperator(
        task_id=task_id,
        name=f"spark-{task_id}",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        service_account_name=SPARK_SA,
        cmds=["/bin/bash", "-c"],
        arguments=[script],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=AWS_SECRET)),
        ],
        env_vars=[
            k8s.V1EnvVar(
                name="POD_IP",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP"),
                ),
            ),
        ],
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=300,
        reattach_on_restart=False,
        do_xcom_push=do_xcom_push,
    )
