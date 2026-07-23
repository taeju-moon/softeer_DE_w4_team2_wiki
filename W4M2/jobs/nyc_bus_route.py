#!/usr/bin/env python3
"""
JFK 통과 버스 노선 → 커버 존 + 배차간격 JSON 생성기
------------------------------------------------------------------
입력:
  - MTA GTFS Static 폴더 (하나 이상). 각 폴더에 routes.txt / trips.txt /
    shapes.txt / stop_times.txt / calendar.txt 가 들어있어야 함.
    예) gtfs_q, gtfs_b, gtfs_busco 를 각각 압축해제한 폴더
  - taxi_zones 셰이프파일 (taxi_zones.shp, EPSG:2263)

출력:
  - route_zone_coverage.json
    [{ "route_name": "Q3",
       "route_long_name": "...",
       "covered_zones": [{"location_id":132,"zone":"JFK Airport","borough":"Queens"}, ...],
       "headway_min": {"peak": 8.5, "offpeak": 15.0, "by_hour": {...}},
       "direction": {...} }, ...]

실행:
  python build_route_zone_json.py \
      --gtfs ./gtfs_q ./gtfs_b ./gtfs_busco \
      --zones ./taxi_zones/taxi_zones.shp \
      --jfk-id 132 \
      --out route_zone_coverage.json

의존성: pip install geopandas shapely pandas pyproj
"""

import argparse
import json
import os
from collections import defaultdict

import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

# 러시아워 정의 (배차 peak/offpeak 구분용). 필요시 조정.
PEAK_HOURS = set(range(7, 10)) | set(range(16, 20))  # 07-09, 16-19시


def gtfs_time_to_hour(t: str) -> int | None:
    """GTFS 시간 'HH:MM:SS'는 24시를 넘길 수 있음(예 25:30:00). hour만 뽑아 24 모듈로."""
    try:
        h = int(t.split(":")[0])
        return h % 24
    except Exception:
        return None


def load_gtfs_tables(gtfs_dirs):
    """여러 GTFS 폴더를 읽어 하나로 합침. route_id 충돌 방지를 위해 폴더 접두사 부여."""
    routes, trips, shapes, stop_times, calendar, agency = [], [], [], [], [], []
    for d in gtfs_dirs:
        tag = os.path.basename(os.path.normpath(d))

        def rd(name, **kw):
            p = os.path.join(d, name)
            if not os.path.exists(p):
                return pd.DataFrame()
            return pd.read_csv(p, dtype=str, **kw)

        r = rd("routes.txt"); t = rd("trips.txt")
        s = rd("shapes.txt"); st = rd("stop_times.txt"); c = rd("calendar.txt")
        a = rd("agency.txt")

        # 폴더 간 id 충돌 방지: 원본 id 보존 + 네임스페이스 컬럼
        for df in (r, t, s, st, c, a):
            if not df.empty:
                df["_src"] = tag
        # agency_id도 폴더 간 충돌 방지를 위해 네임스페이스 부여 (routes ↔ agency 매칭용)
        for df in (r, a):
            if not df.empty and "agency_id" in df.columns:
                df["agency_id"] = tag + "::" + df["agency_id"].astype(str)
        for df, col in [(r, "route_id"), (t, "route_id"), (t, "trip_id"),
                        (t, "shape_id"), (s, "shape_id"), (st, "trip_id")]:
            if not df.empty and col in df.columns:
                df[col] = tag + "::" + df[col].astype(str)

        routes.append(r); trips.append(t); shapes.append(s)
        stop_times.append(st); calendar.append(c); agency.append(a)

    return (pd.concat(routes, ignore_index=True),
            pd.concat(trips, ignore_index=True),
            pd.concat(shapes, ignore_index=True),
            pd.concat(stop_times, ignore_index=True),
            pd.concat(calendar, ignore_index=True) if any(not c.empty for c in calendar) else pd.DataFrame(),
            pd.concat(agency, ignore_index=True) if any(not a.empty for a in agency) else pd.DataFrame())


def build_shape_lines(shapes: pd.DataFrame) -> gpd.GeoDataFrame:
    """shapes.txt → shape_id별 LineString GeoDataFrame (EPSG:4326 → 2263)."""
    shapes = shapes.copy()
    shapes["shape_pt_sequence"] = shapes["shape_pt_sequence"].astype(int)
    shapes["shape_pt_lat"] = shapes["shape_pt_lat"].astype(float)
    shapes["shape_pt_lon"] = shapes["shape_pt_lon"].astype(float)
    shapes = shapes.sort_values(["shape_id", "shape_pt_sequence"])

    geoms, ids = [], []
    for sid, g in shapes.groupby("shape_id"):
        pts = list(zip(g["shape_pt_lon"], g["shape_pt_lat"]))
        if len(pts) >= 2:
            geoms.append(LineString(pts)); ids.append(sid)

    gdf = gpd.GeoDataFrame({"shape_id": ids}, geometry=geoms, crs="EPSG:4326")
    return gdf.to_crs("EPSG:2263")  # taxi_zones 좌표계에 맞춤


def compute_headways(trips: pd.DataFrame, stop_times: pd.DataFrame,
                     calendar: pd.DataFrame) -> dict:
    """
    노선+방향별 배차간격(분) 계산.
    각 trip의 '첫 정류장 출발시각'을 그 trip의 대표 출발시각으로 보고,
    가장 운행이 많은 평일 service_id 하나를 골라 시간대별 출발 횟수 → 60/횟수.
    """
    # 각 trip의 첫 출발시각(stop_sequence 최소) 추출
    st = stop_times[["trip_id", "stop_sequence", "departure_time"]].copy()
    st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce")
    first = (st.sort_values("stop_sequence")
               .groupby("trip_id", as_index=False).first())
    first["hour"] = first["departure_time"].map(gtfs_time_to_hour)

    tt = trips.merge(first[["trip_id", "hour"]], on="trip_id", how="inner")

    # 평일 대표 service_id 고르기 (calendar에 monday=1 인 것 중 trip 최다)
    weekday_services = None
    if not calendar.empty and "monday" in calendar.columns:
        weekday_services = set(calendar[calendar["monday"] == "1"]["service_id"])

    result = {}
    if "direction_id" not in tt.columns:
        tt["direction_id"] = "0"

    for (rid, did), grp in tt.groupby(["route_id", "direction_id"]):
        sub = grp
        if weekday_services is not None and "service_id" in grp.columns:
            wk = grp[grp["service_id"].isin(weekday_services)]
            if not wk.empty:
                sub = wk
        by_hour = sub.dropna(subset=["hour"]).groupby("hour").size()
        if by_hour.empty:
            continue
        hourly_headway = {int(h): round(60.0 / n, 1) for h, n in by_hour.items()}
        peak_trips = sub[sub["hour"].isin(PEAK_HOURS)].shape[0]
        peak_hours_present = len(set(sub.dropna(subset=["hour"])["hour"]) & PEAK_HOURS)
        off_trips = sub[~sub["hour"].isin(PEAK_HOURS)].shape[0]
        off_hours_present = len(set(sub.dropna(subset=["hour"])["hour"]) - PEAK_HOURS)

        result[(rid, str(did))] = {
            "peak": round(60.0 / (peak_trips / peak_hours_present), 1) if peak_hours_present else None,
            "offpeak": round(60.0 / (off_trips / off_hours_present), 1) if off_hours_present else None,
            "by_hour": hourly_headway,
        }
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gtfs", nargs="+", required=True, help="GTFS 폴더들")
    ap.add_argument("--zones", required=True, help="taxi_zones.shp 경로")
    ap.add_argument("--jfk-id", type=int, default=132, help="JFK LocationID")
    ap.add_argument("--out", default="route_zone_coverage.json")
    args = ap.parse_args()

    print(">> GTFS 로드")
    routes, trips, shapes, stop_times, calendar, agency = load_gtfs_tables(args.gtfs)

    # agency_id → agency_name 매핑 (routes.txt에 agency_id, agency.txt에 이름)
    agency_name_by_id = {}
    if not agency.empty and "agency_id" in agency.columns and "agency_name" in agency.columns:
        agency_name_by_id = dict(zip(agency["agency_id"], agency["agency_name"]))

    print(">> 존 폴리곤 로드")
    zones = gpd.read_file(args.zones)
    if zones.crs is None or zones.crs.to_epsg() != 2263:
        zones = zones.to_crs("EPSG:2263")
    jfk_poly = zones[zones["LocationID"] == args.jfk_id].geometry.union_all()

    print(">> shape 라인 생성")
    shape_lines = build_shape_lines(shapes)

    # shape_id → route_id / direction / long_name 매핑
    trip_map = (trips[["shape_id", "route_id", "direction_id"]]
                .drop_duplicates("shape_id"))
    shape_lines = shape_lines.merge(trip_map, on="shape_id", how="left")

    print(">> JFK 통과 노선 자동 탐지")
    # JFK 폴리곤과 교차하는 shape → 그 route가 JFK 경유 노선
    jfk_hit = shape_lines[shape_lines.intersects(jfk_poly)]
    jfk_routes = set(jfk_hit["route_id"].dropna())
    print(f"   JFK 통과 노선 수: {len(jfk_routes)}")

    # 배차 계산
    print(">> 배차간격 계산")
    headways = compute_headways(trips, stop_times, calendar)

    # route 메타
    rmeta = routes.set_index("route_id")
    print(">> 노선별 covered_zones 공간 조인")

    output = []
    for rid in sorted(jfk_routes):
        # 이 노선의 모든 shape 라인을 합쳐 커버 존 계산 (양방향 통합)
        rlines = shape_lines[shape_lines["route_id"] == rid]
        if rlines.empty:
            continue
        merged = rlines.geometry.union_all()
        line_gdf = gpd.GeoDataFrame(geometry=[merged], crs="EPSG:2263")
        hit = gpd.sjoin(zones, line_gdf, predicate="intersects", how="inner")
        covered = (hit[["LocationID", "zone", "borough"]]
                   .drop_duplicates()
                   .sort_values("LocationID"))
        covered_list = [
            {"location_id": int(r.LocationID), "zone": r.zone, "borough": r.borough}
            for r in covered.itertuples()
        ]

        short = rmeta.loc[rid, "route_short_name"] if rid in rmeta.index and "route_short_name" in rmeta.columns else rid.split("::")[-1]
        longn = rmeta.loc[rid, "route_long_name"] if rid in rmeta.index and "route_long_name" in rmeta.columns else ""

        # 운영사(agency): routes.txt의 agency_id → agency.txt의 agency_name
        agency_val = ""
        if rid in rmeta.index and "agency_id" in rmeta.columns:
            aid = rmeta.loc[rid, "agency_id"]
            if isinstance(aid, str):
                agency_val = agency_name_by_id.get(aid, aid.split("::")[-1])

        # 배차 (방향별 존재 → 대표로 dir 0, 없으면 아무거나)
        hw = headways.get((rid, "0")) or next(
            (v for (k_rid, _), v in headways.items() if k_rid == rid), None)

        output.append({
            "route_id": rid.split("::")[-1],
            "route_name": short if isinstance(short, str) else rid.split("::")[-1],
            "route_long_name": longn if isinstance(longn, str) else "",
            "agency": agency_val,               # 정확한 운영사 (예: MTA Bus Company)
            "gtfs_source": rid.split("::")[0],   # 파일 출처 (gtfs_q / gtfs_b / gtfs_busco)
            "covered_zones": covered_list,
            "headway_min": hw,
        })

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f">> 완료: {args.out} ({len(output)}개 노선)")


if __name__ == "__main__":
    main()