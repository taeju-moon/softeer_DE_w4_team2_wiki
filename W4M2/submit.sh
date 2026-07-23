#!/bin/bash
# =====================================================
# submit.sh
#   docker-compose로 띄운 Spark standalone 클러스터에
#   pi.py 작업을 spark-submit으로 제출하는 스크립트
#
# 사용법:
#   ./submit.sh [partitions] [output_path]
#
#   예)
#   ./submit.sh
#   ./submit.sh 10
#   ./submit.sh 10 /opt/spark-data/output/pi_result
# =====================================================

set -e  # 에러 발생 시 즉시 스크립트 중단

# -----------------------------------------------------
# 1. 파라미터 기본값 설정
# -----------------------------------------------------
PARTITIONS=${1:-10}
OUTPUT_PATH=${2:-/opt/spark-data/output/pi_result}
MASTER_URL="spark://spark-master:7077"
APP_PATH="/opt/spark-apps/pi.py"

echo "====================================================="
echo " Spark Job 제출"
echo " - Master       : ${MASTER_URL}"
echo " - App          : ${APP_PATH}"
echo " - Partitions   : ${PARTITIONS}"
echo " - Output Path  : ${OUTPUT_PATH}"
echo "====================================================="

# -----------------------------------------------------
# 2. spark-master 컨테이너가 떠 있는지 확인
# -----------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "[에러] spark-master 컨테이너가 실행 중이 아닙니다."
  echo "먼저 'docker-compose up -d --build' 를 실행하세요."
  exit 1
fi

# -----------------------------------------------------
# 3. spark-master 컨테이너 내부에서 spark-submit 실행
# -----------------------------------------------------
docker exec spark-master spark-submit \
  --master ${MASTER_URL} \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.memory=2g \
  --conf spark.driver.maxResultSize=1g \
  --conf spark.executor.memory=3g \
  --conf spark.executor.cores=2 \
  --conf spark.cores.max=4 \
  --conf spark.sql.shuffle.partitions=8 \
  ${APP_PATH} ${PARTITIONS} ${OUTPUT_PATH}

SUBMIT_EXIT_CODE=$?

if [ ${SUBMIT_EXIT_CODE} -eq 0 ]; then
  echo "====================================================="
  echo " Job 제출 완료. 결과는 다음 경로(로컬 기준 ./data/output)에서 확인:"
  echo " ${OUTPUT_PATH}"
  echo " Spark Web UI: http://localhost:8080"
  echo "====================================================="
else
  echo "[에러] Spark job 실행 중 오류가 발생했습니다. (exit code: ${SUBMIT_EXIT_CODE})"
  echo "docker-compose logs spark-master 로 로그를 확인하세요."
  exit ${SUBMIT_EXIT_CODE}
fi