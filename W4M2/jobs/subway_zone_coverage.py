#!/usr/bin/env python3
"""
지하철역 존(Zone) 커버리지 판별기
------------------------------------------------------------------
입력:
  - GTFS 지하철 stops.txt (location_type=1 인 행만 사용:
    101N/101S 같은 상행/하행 플랫폼이 아닌 대표 역 지점)
  - taxi_zones 셰이프파일 (taxi_zones.shp, EPSG:2263)
  - taxi_zone_lookup.csv (전체 Zone 목록)

출력:
  - subway_zone_coverage.csv
    location_id,zone,borough,has_subway,station_count

실행:
  python subway_zone_coverage.py \
      --stops ./stops.txt \
      --zones ./taxi_zones/taxi_zones.shp \
      --lookup ./taxi_zone_lookup.csv \
      --out subway_zone_coverage.csv

의존성: pip install geopandas shapely pandas pyproj
"""

import argparse

import pandas as pd
import geopandas as gpd


def load_stations(stops_path: str) -> gpd.GeoDataFrame:
    """stops.txt에서 대표 역 지점만 추출 (location_type == 1: 플랫폼 제외)."""
    stops = pd.read_csv(stops_path, dtype=str)
    stations = stops[stops["location_type"] == "1"].copy()
    stations["stop_lat"] = stations["stop_lat"].astype(float)
    stations["stop_lon"] = stations["stop_lon"].astype(float)

    gdf = gpd.GeoDataFrame(
        stations[["stop_id", "stop_name"]],
        geometry=gpd.points_from_xy(stations["stop_lon"], stations["stop_lat"]),
        crs="EPSG:4326",
    )
    return gdf.to_crs("EPSG:2263")  # taxi_zones 좌표계에 맞춤


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stops", required=True, help="GTFS 지하철 stops.txt 경로")
    ap.add_argument("--zones", required=True, help="taxi_zones.shp 경로")
    ap.add_argument("--lookup", required=True, help="taxi_zone_lookup.csv 경로")
    ap.add_argument("--out", default="subway_zone_coverage.csv")
    args = ap.parse_args()

    print(">> 지하철역 로드")
    stations = load_stations(args.stops)
    print(f"   역 수: {len(stations)}")

    print(">> 존 폴리곤 로드")
    zones = gpd.read_file(args.zones)
    if zones.crs is None or zones.crs.to_epsg() != 2263:
        zones = zones.to_crs("EPSG:2263")

    print(">> 역-존 공간 조인 (point-in-polygon)")
    hit = gpd.sjoin(stations, zones, predicate="within", how="inner")
    station_count = hit.groupby("LocationID").size().rename("station_count")

    print(">> taxi_zone_lookup과 병합")
    lookup = pd.read_csv(args.lookup)
    result = lookup.merge(station_count, left_on="LocationID", right_index=True, how="left")
    result["station_count"] = result["station_count"].fillna(0).astype(int)
    result["has_subway"] = result["station_count"] > 0

    result = result.rename(columns={"LocationID": "location_id", "Zone": "zone", "Borough": "borough"})
    result = result[["location_id", "zone", "borough", "has_subway", "station_count"]]
    result = result.sort_values("location_id")

    result.to_csv(args.out, index=False)
    covered = int(result["has_subway"].sum())
    print(f">> 완료: {args.out} (전체 {len(result)}개 존 중 지하철역 존재 {covered}개)")


if __name__ == "__main__":
    main()
