"""JFK 공항 출발 택시 목적지 Zone별 집계 (PySpark).

목적지(도착) Zone 하나당 한 행으로 다음을 뽑는다:

    zone : { 택시 갯수, 주행 거리(mile), 비용 }

- 데이터  : data 디렉터리(컨테이너 기준 /opt/spark-data)에 있는 모든
            yellow_tripdata_*.parquet / fhvhv_tripdata_*.parquet 를 읽는다.
            (특정 월을 받아오는 대신, 폴더에 이미 있는 parquet 전체가 대상)
- 출발지  : JFK 공항 (PULocationID = 132)
- 서비스  : Yellow / HVFHV 통합(합산)
- 산출물  : output/jfk_zone_stats/part-*.csv (coalesce(1) 단일 CSV)

실행(예):
    ./submit.sh jfk_zone_stats.py
"""
from __future__ import annotations

import argparse
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
JFK_LOCATION_ID = 132                 # JFK 공항 Zone

# HVFHV 사업자 라이선스 번호 -> 사업자명
OPERATOR_MAP = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}


# ---------------------------------------------------------------------------
# 0. 인자
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="JFK 출발 목적지 Zone별 집계")
    p.add_argument("--data-dir", default="/opt/spark-data", help="parquet 파일들이 있는 디렉터리")
    p.add_argument("--output", default="/opt/spark-output/jfk_zone_stats", help="결과 CSV 출력 경로(디렉토리)")
    p.add_argument("--outlier-pct", type=float, default=1.0, help="상·하위 이상치 컷오프(%)")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# 1. 입력 탐색 (data-dir 안의 parquet 전체 + zone lookup csv, 전부 로컬 파일 필수)
# ---------------------------------------------------------------------------
def find_lookup(data_dir: str) -> str:
    path = os.path.join(data_dir, "taxi_zone_lookup.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} 가 없습니다. taxi_zone_lookup.csv를 {data_dir}에 준비하세요.")
    return path


def discover(data_dir: str):
    """data_dir 안의 yellow/hvfhv parquet를 glob 패턴으로 찾는다.

    경로 패턴(와일드카드)을 그대로 spark.read.parquet 에 넘기면 매칭되는
    파일/블록 단위로 파티션이 나뉘어 executor들에 분산 -> 동시에 읽힌다.
    """
    yellow_glob = os.path.join(data_dir, "yellow_tripdata_*.parquet")
    hvfhv_glob = os.path.join(data_dir, "fhvhv_tripdata_*.parquet")
    yellow_files = sorted(glob.glob(yellow_glob))
    hvfhv_files = sorted(glob.glob(hvfhv_glob))
    if not yellow_files and not hvfhv_files:
        raise FileNotFoundError(f"{data_dir} 안에 yellow/fhvhv parquet 파일이 없습니다.")
    print(f"[discover] yellow {len(yellow_files)}개, hvfhv {len(hvfhv_files)}개")
    return yellow_glob if yellow_files else None, hvfhv_glob if hvfhv_files else None


# ---------------------------------------------------------------------------
# 2. 스키마 통합 (Yellow / HVFHV -> 공통 컬럼)
# ---------------------------------------------------------------------------
def unify(yellow_raw, hvfhv_raw):
    """yellow_raw/hvfhv_raw는 데이터가 없으면 None일 수 있다."""
    parts = []
    if yellow_raw is not None:
        parts.append(
            yellow_raw.select(
                F.lit("yellow").alias("service_type"),
                F.lit("yellow").alias("operator"),
                F.col("tpep_pickup_datetime").alias("pickup_datetime"),
                F.col("PULocationID").cast("long").alias("pu_location_id"),
                F.col("DOLocationID").cast("long").alias("do_location_id"),
                F.col("trip_distance").cast("double").alias("trip_distance"),   # mile
                F.col("fare_amount").cast("double").alias("fare_amount"),
            )
        )

    if hvfhv_raw is not None:
        op_map = F.create_map([F.lit(x) for kv in OPERATOR_MAP.items() for x in kv])
        parts.append(
            hvfhv_raw.select(
                F.lit("hvfhv").alias("service_type"),
                F.coalesce(op_map[F.col("hvfhs_license_num")], F.col("hvfhs_license_num")).alias("operator"),
                F.col("pickup_datetime").alias("pickup_datetime"),
                F.col("PULocationID").cast("long").alias("pu_location_id"),
                F.col("DOLocationID").cast("long").alias("do_location_id"),
                F.col("trip_miles").cast("double").alias("trip_distance"),      # mile
                F.col("base_passenger_fare").cast("double").alias("fare_amount"),
            )
        )

    df = parts[0]
    for part in parts[1:]:
        df = df.unionByName(part)
    return df


# ---------------------------------------------------------------------------
# 3. 정제: JFK 필터 -> 결측/논리오류 제거 -> service_type별 이상치 제거
# ---------------------------------------------------------------------------
def clean(df, outlier_pct: float):
    df = df.filter(F.col("pu_location_id") == JFK_LOCATION_ID)
    df = df.dropna(subset=["do_location_id", "trip_distance", "fare_amount"])
    df = df.filter((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))

    lo_q, hi_q = outlier_pct / 100.0, 1.0 - outlier_pct / 100.0
    service_types = [r["service_type"] for r in df.select("service_type").distinct().collect()]
    result = None
    for st in service_types:
        sub = df.filter(F.col("service_type") == st)
        cond = F.lit(True)
        for c in ("trip_distance", "fare_amount"):
            lo, hi = sub.approxQuantile(c, [lo_q, hi_q], 0.01)
            cond = cond & (F.col(c) >= F.lit(lo)) & (F.col(c) <= F.lit(hi))
        sub = sub.filter(cond)
        result = sub if result is None else result.unionByName(sub)
    return result if result is not None else df


# ---------------------------------------------------------------------------
# 4. 목적지 Zone별 집계: { 택시 갯수, 주행 거리(mile), 비용 }
# ---------------------------------------------------------------------------
def aggregate(df, lookup):
    names = lookup.select(
        F.col("LocationID").alias("_lk_id"),
        F.col("Zone").alias("zone"),
        F.col("Borough").alias("borough"),
    )
    agg = df.groupBy("do_location_id").agg(
        F.count(F.lit(1)).alias("trip_count"),                     # 택시 갯수
        F.avg("trip_distance").alias("avg_distance_mi"),           # 주행 거리 (평균, mile)
        F.sum("trip_distance").alias("total_distance_mi"),         # 주행 거리 (합계, mile)
        F.avg("fare_amount").alias("avg_fare"),                    # 비용 (평균)
        F.sum("fare_amount").alias("total_fare"),                  # 비용 (합계)
        F.percentile_approx("fare_amount", 0.5).alias("fare_median"),  # 비용 (중앙값)
    )
    return (
        agg.join(names, agg["do_location_id"] == names["_lk_id"], "left")
        .drop("_lk_id")
        .select(
            "do_location_id", "zone", "borough", "trip_count",
            "avg_distance_mi", "total_distance_mi",
            "avg_fare", "total_fare", "fare_median",
        )
        .orderBy(F.col("trip_count").desc())
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main(argv=None):
    args = parse_args(argv)

    yellow_glob, hvfhv_glob = discover(args.data_dir)
    lookup_path = find_lookup(args.data_dir)

    spark = SparkSession.builder.appName("jfk-zone-stats").getOrCreate()

    # 와일드카드 경로 하나를 넘겨도 매칭되는 모든 parquet 파일이 여러 파티션으로
    # 나뉘어 spark-worker 들에 동시에 읽힌다(파일 목록을 만들어 순차로 읽지 않음).
    yellow_raw = spark.read.parquet(yellow_glob) if yellow_glob else None
    hvfhv_raw = spark.read.parquet(hvfhv_glob) if hvfhv_glob else None
    lookup = spark.read.option("header", True).option("inferSchema", True).csv(lookup_path)

    df = clean(unify(yellow_raw, hvfhv_raw), args.outlier_pct)
    result = aggregate(df, lookup)

    # 목적지 Zone 수(수백 행)라 단일 파일로 저장 (coalesce(1))
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)

    print(f"[done] 목적지 Zone별 집계 -> {args.output} (내부 part-*.csv)")
    result.show(15, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
