# JFK 출발 택시 데이터 수집·정제 및 목적지·요금 분석 설계

- 작성일: 2026-07-23
- 범위: 요구사항명세서 팀 역할 **1(데이터 수집/정제)** + **2(목적지/요금 분석)**
- 대상 기간: **2026년 1~5월** (다운로드 시점 공개분 전부; 6월 이후 미공개)
- 상위 요구사항: [W4M2/요구사항명세서.md](../../../W4M2/요구사항명세서.md)

## 1. 목표와 범위

JFK 공항(PULocationID=132)에서 출발하는 Yellow Taxi + HVFHV 트립을 수집·정제하고,
목적지 Zone별 이용량·요금을 집계한 뒤, 인기 Zone을 지리적으로 K-means 클러스터링하여
"노선 후보 corridor"를 도출한다. 산출물은 CSV로 팀에 전달한다.

**이 설계에 포함되지 않는 것** (팀 다른 역할):
- 버스 GTFS 배차 데이터 파싱 및 수요-공급 갭 분석 (역할 3)
- 최종 시각화 및 데이터 상품 리포트 (역할 4)

## 2. 아키텍처

독립 실행·테스트 가능한 **모듈 3개**로 분리한다.

```
[module 1] ingest_clean.py (Spark)      [module 2] zone_analysis.py (Spark)     [module 3] cluster_corridors.py (pandas/sklearn/geopandas)
────────────────────────────────       ───────────────────────────────────     ────────────────────────────────────────
TLC 5개월 다운로드                       data/cleaned/ (Parquet) 로드            output/zone_aggregation.csv 로드
  yellow 2026-01~05                          │                                       +
  fhvhv  2026-01~05                     taxi_zone_lookup.csv 조인               taxi_zones shapefile 중심좌표
       │                                     │                                       │
  통합 스키마 매핑 후 union               service_zone='Airports' 목적지 제외      파레토 상위 Zone (cum_pct<=0.80)
       │                                     │                                       │
  JFK(PULocationID=132) 필터            Zone별 / 시간대별 집계                   K-means (trip_count 가중 옵션)
       │                                ┌────┴────┐                                   │
  결측/논리오류/이상치 제거              zone_agg   hourly_agg                   corridor_clusters.csv
       │                                 .csv       .csv
  data/cleaned/ (Parquet, 중간산출물)
```

- **Module 1 → 2** 사이 중간 산출물: **Parquet** (`data/cleaned/`, `service_type` 파티션) — 재실행/재현성용.
- **Module 2, 3** 최종 집계: **CSV** (`output/`) — 팀 downstream(pandas/Jupyter) 및 엑셀 확인용.
- Module 3은 geopandas/sklearn이 설치된 **Jupyter 이미지 환경**에서 실행하는 독립 스크립트.
- Spark 두 단계 분리 이유: 다운로드·정제(무겁고 재실행 드묾)와 집계(반복 조정)를 분리해 재실행 비용 절감, 역할 분담 용이.

## 3. 데이터 소스

| 데이터 | URL 패턴 |
|---|---|
| Yellow Taxi | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-{01..05}.parquet` |
| HVFHV | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2026-{01..05}.parquet` |
| Zone lookup | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` |
| Zones shapefile | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip` |

- 2026-01~05 (Yellow + HVFHV) 존재 확인 완료(HTTP 200). 2026-06 이후는 403(미공개).
- raw 총량 약 2.8GB (Yellow ~325MB + HVFHV ~2.5GB). JFK 필터 후 급감.

## 4. Module 1 — `ingest_clean.py` (Spark)

### 4.1 통합 스키마 매핑

| 통합 컬럼 | Yellow Taxi | HVFHV |
|---|---|---|
| `service_type` | `'yellow'` (리터럴) | `'hvfhv'` (리터럴) |
| `operator` | `'yellow'` | `hvfhs_license_num` 매핑 (HV0002=Juno, HV0003=Uber, HV0004=Via, HV0005=Lyft) |
| `pickup_datetime` | `tpep_pickup_datetime` | `pickup_datetime` |
| `dropoff_datetime` | `tpep_dropoff_datetime` | `dropoff_datetime` |
| `pu_location_id` | `PULocationID` | `PULocationID` |
| `do_location_id` | `DOLocationID` | `DOLocationID` |
| `trip_distance` | `trip_distance` | `trip_miles` |
| `fare_amount` | `fare_amount` | `base_passenger_fare` |
| `trip_duration_min` | `(dropoff_datetime - pickup_datetime)` 초 → /60 | `trip_time / 60` |

> 실제 2026 parquet 컬럼명은 module 1 실행 초반 **스키마 검증(assert)** 으로 대조.
> 명세서 기준으로 작성하되, 컬럼명이 다르면 fail-fast로 중단하고 매핑을 수정한다.
> `fare_amount ↔ base_passenger_fare`는 명세서 매핑을 따른다(완전 동일 정의는 아님을 명시).

### 4.2 정제 순서 (각 단계 후 잔존 건수 로깅)

1. **JFK 필터**: `pu_location_id == 132` (다운로드 직후 최우선 → 이후 연산 경량화)
2. **필수 필드 결측 제거**: `do_location_id`, `fare_amount`, `trip_distance`, `pickup_datetime`, `dropoff_datetime`
3. **논리 오류 제거**: `dropoff_datetime <= pickup_datetime`, `fare_amount <= 0`, `trip_distance <= 0`
4. **통계 이상치 제거**: `trip_distance`, `trip_duration_min`, `fare_amount`에 대해
   **`service_type`별 상·하위 1% 백분위 컷오프** 적용 (Yellow/HVFHV 분포 상이하므로 분리 산정,
   `approxQuantile` 사용)
5. **저장**: `data/cleaned/`에 Parquet, `service_type`로 파티션

### 4.3 파라미터
- 월 목록(기본 `2026-01`..`2026-05`), 이상치 컷오프 %(기본 1.0), 출력 경로 — 스크립트 인자/상단 상수로 노출
- `service_zone='Airports'` 목적지 제외는 여기서 **하지 않음** (중간셋은 원본 목적지 보존; module 2에서 제외)

## 5. Module 2 — `zone_analysis.py` (Spark)

`data/cleaned/` Parquet 로드 → `taxi_zone_lookup.csv`를 `do_location_id`로 조인 →
목적지의 `service_zone == 'Airports'` 행 제외 → 집계.

### 5.1 `output/zone_aggregation.csv` (목적지 Zone별)
컬럼: `do_location_id`, `zone`, `borough`,
`trip_count`, `avg_duration_min`, `avg_distance`,
`fare_mean`, `fare_median`, `fare_var`,
`trip_pct`(전체 대비 비중), `cum_pct`(trip_count 내림차순 누적 — 파레토용).
- 전체(service_type 합산) 기준 집계 + `service_type`별 분리 집계 병행.

### 5.2 `output/hourly_aggregation.csv` (시간대별)
컬럼: `hour`(0–23), `do_location_id`, `trip_count`
- `pickup_datetime`의 hour 기준. downstream corridor 피크 분석 근거.
- 목적지 무관 전체 시간대 요약(`hour`, `trip_count`)도 별도 포함.

## 6. Module 3 — `cluster_corridors.py` (pandas/sklearn/geopandas)

1. `output/zone_aggregation.csv`에서 파레토 상위 Zone 추출 (`cum_pct <= 0.80`)
2. `taxi_zones` shapefile 로드 → 좌표계 변환(EPSG:2263 → EPSG:4326) → Zone 중심좌표(centroid) 계산
3. 상위 Zone 중심좌표에 **K-means** 적용
   - K는 elbow(inertia)로 후보 제시, 기본값 파라미터로 노출
   - `trip_count` 가중 옵션 (sample_weight)
4. **`output/corridor_clusters.csv`**: `do_location_id`, `zone`, `cluster_id`, `centroid_lat`, `centroid_lon`, `trip_count`

## 7. 인프라 변경

- `submit.sh`: 현재 `APP_PATH=/opt/spark-apps/pi.py` 고정 → **job 스크립트명을 인자로** 받도록 일반화
  (예: `./submit.sh ingest_clean.py`). 기존 사용법 하위호환 유지.
- `jobs/a.py`(빈 파일)는 제거 또는 실제 job 스크립트로 대체.

## 8. 테스트 전략 (TDD)

- 소규모 **합성 Parquet fixture**(Yellow/HVFHV 각 몇 행, JFK/비JFK/이상치 혼합)로:
  - Module 1: 스키마 통합, JFK 필터, 결측/논리/이상치 제거가 기대 건수를 남기는지 단위 검증
  - Module 2: 조인·Airports 제외·집계값·파레토 누적비율 계산 검증
  - Module 3: 파레토 상위 추출, centroid 계산, K-means 출력 형태 검증
- 실제 다운로드 대용량 실행은 통합 확인용(스키마 assert 포함).

## 9. 산출물 목록

1. `jobs/ingest_clean.py` — Spark 수집·정제 배치
2. `jobs/zone_analysis.py` — Spark 집계 배치
3. `jobs/cluster_corridors.py` — 파이썬 클러스터링 스크립트
4. 중간: `data/cleaned/*.parquet`
5. 최종: `output/zone_aggregation.csv`, `output/hourly_aggregation.csv`, `output/corridor_clusters.csv`
6. 테스트 코드 및 fixture

## 10. 성공 기준

- 2026-01~05 JFK 출발 Yellow+HVFHV 통합 정제셋이 재현 가능하게 생성됨
- 목적지 Zone별 집계·파레토 상위 Zone·시간대별 집계 CSV 생성
- 인기 Zone K-means corridor CSV 생성 (다운스트림 버스 갭 분석에 바로 투입 가능한 형태)
