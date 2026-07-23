# =====================================================
# Spark Standalone Cluster - Base Image
# Java + Python + Apache Spark 설치
# 이 이미지 하나로 master / worker 컨테이너를 모두 띄운다
# =====================================================

FROM eclipse-temurin:11-jdk-jammy

# -----------------------------------------------------
# 버전 변수
# -----------------------------------------------------
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# -----------------------------------------------------
# 1. 필수 패키지 설치: python3.11, pip, wget, procps
#    - jammy(Ubuntu 22.04) 기본 apt python3는 3.10인데,
#      jupyter/pyspark-notebook:spark-3.5.0 이미지(드라이버)는 Python 3.11을 사용함.
#    - PySpark는 driver/executor의 Python 마이너 버전이 다르면 실행 자체를 막으므로
#      (PYTHON_VERSION_MISMATCH), deadsnakes PPA로 3.11을 명시적으로 설치해서 맞춘다.
# -----------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    wget \
    curl \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-distutils \
    python3.11-venv \
    && rm -rf /var/lib/apt/lists/*

# python3.11을 기본 python/python3로 연결
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

# get-pip으로 python3.11용 pip 설치
RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3.11 /tmp/get-pip.py \
    && rm /tmp/get-pip.py

# -----------------------------------------------------
# 2. Apache Spark 다운로드 및 설치
# -----------------------------------------------------
RUN wget --no-verbose \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# -----------------------------------------------------
# 3. pyspark 등 파이썬 의존성 설치
# -----------------------------------------------------
RUN python3.11 -m pip install --no-cache-dir pyspark==${SPARK_VERSION}

# -----------------------------------------------------
# 4. 작업 디렉토리 준비
#    - /opt/spark-apps : job 스크립트(pi.py 등) 위치
#    - /opt/spark-data  : 입력/출력 데이터 마운트 위치
# -----------------------------------------------------
RUN mkdir -p /opt/spark-apps /opt/spark-data

WORKDIR ${SPARK_HOME}

# Spark Web UI(8080, 4040) / Master 통신(7077) 포트 노출
EXPOSE 8080 7077 4040 8081

# 컨테이너 시작 시 기본 커맨드는 없음 -> docker-compose에서 role별로 command 지정
CMD ["bash"]