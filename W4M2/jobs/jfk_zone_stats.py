"""JFK 공항 출발 택시 목적지 Zone별 집계 (PySpark).

목적지(도착) Zone 하나당 한 행으로 다음을 뽑는다:

    zone : { 택시 갯수, 주행 km, 비용 }

- 데이터  : 2026년 Yellow Taxi + HVFHV(Uber/Lyft 등), TLC 공개분(1~5월)
- 출발지  : JFK 공항 (PULocationID = 132)
- 서비스  : Yellow / HVFHV 통합(합산)
- 산출물  : output/jfk_zone_stats/part-*.csv (coalesce(1) 단일 CSV)

실행(예):
    ./submit.sh jfk_zone_stats.py                     # 2026-01~05 전체
    ./submit.sh jfk_zone_stats.py --months 2026-01    # 단일 월
"""
from __future__ import annotations

import argparse
import os
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
JFK_LOCATION_ID = 132                 # JFK 공항 Zone
MILES_TO_KM = 1.60934                 # TLC 거리 단위(mile) -> km

# 다운로드 시점(2026-07) 기준 공개된 2026년 월. 6월 이후는 미공개.
DEFAULT_MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]

# HVFHV 사업자 라이선스 번호 -> 사업자명
OPERATOR_MAP = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}


# ---------------------------------------------------------------------------
# 0. 인자
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="JFK 출발 목적지 Zone별 집계")
    p.add_argument("--months", nargs="+", default=DEFAULT_MONTHS, help="대상 월(YYYY-MM)")
    p.add_argument("--data-dir", default="/opt/spark-data", help="raw 데이터 루트")
    p.add_argument("--output", default="/opt/spark-output/jfk_zone_stats", help="결과 CSV 출력 경로(디렉토리)")
    p.add_argument("--outlier-pct", type=float, default=1.0, help="상·하위 이상치 컷오프(%)")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# 1. 다운로드 (없으면 받고, 있으면 재사용)
# ---------------------------------------------------------------------------
def download(url: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, os.path.basename(url))
    if not (os.path.exists(dest) and os.path.getsize(dest) > 0):
        print(f"[download] {url}")
        urllib.request.urlretrieve(url, dest)
    return dest


def download_all(months, raw_dir):
    yellow, hvfhv = [], []
    for m in months:
        yellow.append(download(f"{BASE_URL}/trip-data/yellow_tripdata_{m}.parquet", raw_dir))
        hvfhv.append(download(f"{BASE_URL}/trip-data/fhvhv_tripdata_{m}.parquet", raw_dir))
    lookup = download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", raw_dir)
    return yellow, hvfhv, lookup


# ---------------------------------------------------------------------------
# 2. 스키마 통합 (Yellow / HVFHV -> 공통 컬럼)
# ---------------------------------------------------------------------------
def unify(yellow_raw, hvfhv_raw):
    yellow = yellow_raw.select(
        F.lit("yellow").alias("service_type"),
        F.lit("yellow").alias("operator"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("PULocationID").cast("long").alias("pu_location_id"),
        F.col("DOLocationID").cast("long").alias("do_location_id"),
        F.col("trip_distance").cast("double").alias("trip_distance"),   # mile
        F.col("fare_amount").cast("double").alias("fare_amount"),
    )

    op_map = F.create_map([F.lit(x) for kv in OPERATOR_MAP.items() for x in kv])
    hvfhv = hvfhv_raw.select(
        F.lit("hvfhv").alias("service_type"),
        F.coalesce(op_map[F.col("hvfhs_license_num")], F.col("hvfhs_license_num")).alias("operator"),
        F.col("pickup_datetime").alias("pickup_datetime"),
        F.col("PULocationID").cast("long").alias("pu_location_id"),
        F.col("DOLocationID").cast("long").alias("do_location_id"),
        F.col("trip_miles").cast("double").alias("trip_distance"),      # mile
        F.col("base_passenger_fare").cast("double").alias("fare_amount"),
    )

    return yellow.unionByName(hvfhv)


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
# 4. 목적지 Zone별 집계: { 택시 갯수, 주행 km, 비용 }
# ---------------------------------------------------------------------------
def aggregate(df, lookup):
    names = lookup.select(
        F.col("LocationID").alias("_lk_id"),
        F.col("Zone").alias("zone"),
        F.col("Borough").alias("borough"),
    )
    agg = df.groupBy("do_location_id").agg(
        F.count(F.lit(1)).alias("trip_count"),                              # 택시 갯수
        (F.avg("trip_distance") * MILES_TO_KM).alias("avg_distance_km"),    # 주행 km (평균)
        (F.sum("trip_distance") * MILES_TO_KM).alias("total_distance_km"),  # 주행 km (합계)
        F.avg("fare_amount").alias("avg_fare"),                             # 비용 (평균)
        F.sum("fare_amount").alias("total_fare"),                           # 비용 (합계)
        F.percentile_approx("fare_amount", 0.5).alias("fare_median"),       # 비용 (중앙값)
    )
    return (
        agg.join(names, agg["do_location_id"] == names["_lk_id"], "left")
        .drop("_lk_id")
        .select(
            "do_location_id", "zone", "borough", "trip_count",
            "avg_distance_km", "total_distance_km",
            "avg_fare", "total_fare", "fare_median",
        )
        .orderBy(F.col("trip_count").desc())
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main(argv=None):
    args = parse_args(argv)
    raw_dir = os.path.join(args.data_dir, "raw")

    yellow_paths, hvfhv_paths, lookup_path = download_all(args.months, raw_dir)

    spark = SparkSession.builder.appName("jfk-zone-stats").getOrCreate()

    yellow_raw = spark.read.parquet(*yellow_paths)
    hvfhv_raw = spark.read.parquet(*hvfhv_paths)
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
