#!/bin/bash
# =====================================================
# submit.sh
#   docker-compose로 띄운 Spark standalone 클러스터에
#   jobs/ 안의 파이썬 스크립트를 spark-submit으로 제출하는 스크립트
#
# 사용법:
#   ./submit.sh <script.py> [script args...]
#
#   예)
#   ./submit.sh jfk_zone_stats.py
#   ./submit.sh jfk_zone_stats.py --outlier-pct 2.0
#   ./submit.sh jfk_zone_stats.py --data-dir /opt/spark-data --output /opt/spark-output/jfk_zone_stats
#
#   spark-submit 설정(리소스 등)은 아래 환경변수로 덮어쓸 수 있음:
#   EXECUTOR_MEMORY=4g CORES_MAX=8 ./submit.sh jfk_zone_stats.py
# =====================================================

set -e  # 에러 발생 시 즉시 스크립트 중단

# -----------------------------------------------------
# 1. 파라미터 기본값 설정 (환경변수로 오버라이드 가능)
# -----------------------------------------------------
SCRIPT_NAME=${1:?"사용법: ./submit.sh <script.py> [script args...]"}
shift
SCRIPT_ARGS=("$@")

MASTER_CONTAINER="${MASTER_CONTAINER:-spark-master}"
MASTER_URL="${MASTER_URL:-spark://spark-master:7077}"
APPS_DIR="${APPS_DIR:-/opt/spark-apps}"
APP_PATH="${APPS_DIR}/${SCRIPT_NAME}"

# spark-submit 리소스/설정 (필요 시 환경변수로 덮어쓰기)
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
DRIVER_MAX_RESULT_SIZE="${DRIVER_MAX_RESULT_SIZE:-1g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-3g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
CORES_MAX="${CORES_MAX:-4}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-8}"

echo "====================================================="
echo " Spark Job 제출"
echo " - Master       : ${MASTER_URL}"
echo " - App          : ${APP_PATH}"
echo " - Args         : ${SCRIPT_ARGS[*]}"
echo "====================================================="

# -----------------------------------------------------
# 2. spark-master 컨테이너가 떠 있는지 확인
# -----------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -q "^${MASTER_CONTAINER}$"; then
  echo "[에러] ${MASTER_CONTAINER} 컨테이너가 실행 중이 아닙니다."
  echo "먼저 'docker-compose up -d --build' 를 실행하세요."
  exit 1
fi

# -----------------------------------------------------
# 3. spark-master 컨테이너 내부에서 spark-submit 실행
#    (set -e 에서도 실패 시 exit code를 잡을 수 있도록 if로 실행)
# -----------------------------------------------------
if docker exec "${MASTER_CONTAINER}" spark-submit \
  --master "${MASTER_URL}" \
  --deploy-mode client \
  --conf spark.driver.host="${MASTER_CONTAINER}" \
  --conf spark.driver.memory="${DRIVER_MEMORY}" \
  --conf spark.driver.maxResultSize="${DRIVER_MAX_RESULT_SIZE}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY}" \
  --conf spark.executor.cores="${EXECUTOR_CORES}" \
  --conf spark.cores.max="${CORES_MAX}" \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS}" \
  "${APP_PATH}" "${SCRIPT_ARGS[@]}"; then
  echo "====================================================="
  echo " Job 제출 완료. 결과는 각 스크립트의 --output(또는 기본값) 경로"
  echo " (로컬 기준 ./output, 컨테이너 기준 /opt/spark-output)에서 확인하세요."
  echo " Spark Web UI: http://localhost:8080"
  echo "====================================================="
else
  SUBMIT_EXIT_CODE=$?
  echo "[에러] Spark job 실행 중 오류가 발생했습니다. (exit code: ${SUBMIT_EXIT_CODE})"
  echo "docker-compose logs spark-master 로 로그를 확인하세요."
  exit ${SUBMIT_EXIT_CODE}
fi