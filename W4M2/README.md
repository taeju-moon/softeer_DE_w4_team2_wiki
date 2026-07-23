# W4M2 2조 팀활동

> **W4M2 ** — "자율주행차 데이터라면 어떤 Data Product를 만들 수 있을까?"

---

## 1. 과제 요구사항

기존 개인 과제는 Apache Spark로 NYC TLC(Taxi and Limousine Commission) 택시 운행 데이터를 분석하는 것이었다.
여기에 팀 활동 주제가 더해졌다.

> **"이 데이터가 사람이 운행하는 차량이 아니라 '자율주행차' 데이터라면, 어떤 Data Product를 만들면 좋을까?"**

즉, 지금 우리가 보고 있는 승하차/요금/경로 데이터가 앞으로 자율주행 택시(로보택시) 운행 데이터로 대체된다고 가정하고, 그 위에서 성립하는 비즈니스 아이디어를 수립하고 프로토타입까지 만들어 보는 것이 목표다.

---

## 2. 팀 아이디어 브레인스토밍

논의 과정에서 나온 의견들. ~~취소선~~ 은 검토 후 기각된 아이디어, **굵게** 표시된 것이 최종 채택 방향이다.

#### 기각된 아이디어
- 과거 사람이 운전한 택시 데이터와 비교하여 운행 경로 선택 및 소모 시간 비교
- 자율주행 의사결정 알고리즘 ↔ 실제 이동 데이터/결과를 통해 알고리즘 튜닝
- 트롤리 딜레마 분석 — 사고 시 어떤 판단이 리스크가 최소였는지 검토
- 반복적으로 같은 경로를 이용하는 고객 대상 자율주행 기반 카풀 기능 제공

#### 검토된 유효 아이디어 (B2B 계열)
- **Zone별 수요-공급 히트맵**: 시간대/요일별로 어느 taxi zone에서 승차 요청이 몰리는지 예측해, 차량을 미리 재배치(rebalancing)하는 대시보드 (B2B)
- **수요 예측 기반 자율주행 택시 재배치**: 날씨·피크 시간대 수요를 예측해 어떤 차가 어디로 이동해야 하는지 제시 (B2B)
- **자율주행 사고 이력 기반 보험 설계**: 탑승객 대상 보험 상품 설계. 혹은 사고 다발 지점을 분석해 택시 회사에 판매.
  예) 자율주행차가 야간에 사고가 많다 → 야간 전용 보험 상품 제공
- ** (최종 채택) 공항 출발 택시 목적지 분석 → 버스 노선 운영 제안**:
  공항에서 출발하는 택시의 목적지별 이용량·요금 분포를 분석하고, 자주 이용되는 구간을 도출하여 **버스 회사에 노선 운영(배차 증편/신설)을 제안**하는 데이터 상품.

> **참고**: 이 마지막 아이디어는 원래 "자주 이용되는 구간에 적합한 **멤버십 요금제**를 설계·시뮬레이션하는 서비스"였으나, 논의 과정에서 멤버십보다 **버스 회사에게 노선 운영을 제안**하는 방향이 더 실효성 있다고 판단하여 주제가 변경되었다.

### 2.1 최종 채택 주제와 선정 이유

> **"JFK 공항 출발 택시 목적지별 수요·요금 분석 기반 버스 노선 배차 개선 제안 서비스"**

**선정 이유**
1. **비즈니스 타깃이 명확하다.** "버스 회사"라는 구체적인 고객이 존재하고, 제안 형태(노선 증편/신설)가 actionable하다.
2. **필요한 데이터를 쉽게 확보할 수 있다.** 뉴욕 버스 노선 데이터를 MTA Open Data(GTFS)로 무료로 구할 수 있어, 택시 수요 데이터와 버스 공급 데이터를 실제로 대조할 수 있다.

**핵심 시나리오**
> 자율주행 택시가 공항에서 특정 지역으로 매우 많이 이동하는데, 그 구간의 버스 배차가 수요를 못 따라간다면 → 해당 노선의 증편/신설을 버스 회사에 제안한다.

---

## 3. 데이터 수집

### 3.1 수집 기간
- **2026년 1월 ~ 5월** (5개월치)

### 3.2 택시 데이터 — NYC TLC Trip Record Data
- 출처: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- 아래 **두 종류**의 Parquet 파일만 수집 대상으로 삼았다. (월별 파일, 2026-01 ~ 2026-05)
  - Yellow Taxi Trip Records — 예: `yellow_tripdata_2026-01.parquet`
  - High Volume For-Hire Vehicle(HVFHV) Trip Records — 예: `fhvhv_tripdata_2026-01.parquet`
- 다운로드 URL 패턴: `https://d37ci6vzurychx.cloudfront.net/trip-data/{yellow|fhvhv}_tripdata_2026-0{1..5}.parquet`

| 구분 | 무엇인가 | 왜 포함했나 |
|---|---|---|
| **Yellow Taxi** | 뉴욕시 어디서나 길거리 호출(street hail)이 가능한 전통적인 노란 택시. 미터기 요금 기반이며 JFK↔맨해튼은 flat fare 구조를 가진다. | JFK 지상교통의 전통적 분석 축이자 개인 과제부터 이어진 대상. |
| **HVFHV** (High Volume For-Hire Vehicle) | Uber, Lyft 등 앱 기반 대형 승차공유 서비스. 하루 1만 건 이상 운행하는 사업자가 TLC에 별도 리포팅하는 트립 기록. `hvfhs_license_num`으로 사업자를 구분한다. (HV0002 Juno, HV0003 Uber, HV0004 Via, HV0005 Lyft) | JFK 지상교통에서 Yellow Taxi 점유율이 크게 하락(과거 60%대 → 최근 약 18%)했고 나머지 대부분을 HVFHV가 차지하고 있음. Yellow만으로는 전체 수요의 일부만 반영되어 분석 신뢰도가 떨어지므로 함께 수집. |

> 참고: Green Taxi(Boro Taxi)는 규정상 공항 픽업이 금지되어 JFK발 데이터가 거의 없고, 일반 FHV(리무진/블랙카)는 요금 필드 누락이 많아 두 종류 모두 수집 대상에서 제외했다.

### 3.3 버스 데이터 — MTA GTFS Static
- 출처: <https://www.mta.info/developers>
- MTA가 배포하는 **GTFS(General Transit Feed Specification) Static** 데이터. 노선/정류장/시간표를 표준 포맷의 여러 `.txt` 파일로 제공한다.
- borough 단위로 나뉜 피드를 수집: `gtfs_b`(브루클린), `gtfs_q`(퀸즈), `gtfs_bx`(브롱크스), `gtfs_m`(맨해튼), `gtfs_si`(스태튼아일랜드), `gtfs_busco`(MTA Bus Company).
- 저장 위치: `data/raw/gtfs/gtfs_*/`

### 3.4 보조 데이터 — Taxi Zone
- `taxi_zone_lookup.csv`: `LocationID → Zone / Borough` 이름 매핑 테이블 (TLC 제공)
- `taxi_zones` shapefile: Zone 경계 폴리곤(GIS). 좌표계 EPSG:2263.
  - 위치: `data/raw/taxi_zones/` (`taxi_zones.shp`, `.dbf`, `.shx`, `.prj`, `.cpg`)
- 택시 데이터의 출발/목적지는 위경도가 아니라 **Zone ID**로만 주어지므로, 이 매핑 데이터가 택시-버스를 연결하는 공통 키가 된다.

---

## 4. 데이터 구조

### 4.1 Yellow Taxi (`yellow_tripdata_*.parquet`)
본 프로젝트에서 사용하는 주요 컬럼:

| 컬럼 | 설명 | 프로젝트에서의 용도 |
|---|---|---|
| `tpep_pickup_datetime` | 승차 시각 | 시간대별 피크 분석 |
| `tpep_dropoff_datetime` | 하차 시각 | (trip duration) |
| `PULocationID` | 승차 Zone ID | **JFK(132) 필터 기준** |
| `DOLocationID` | 하차 Zone ID | 목적지별 집계 키 |
| `trip_distance` | 주행 거리 (mile) | 거리 분석 |
| `fare_amount` | 미터 요금 (USD) | 요금 분포 분석 |

(그 외 `passenger_count`, `tip_amount`, `total_amount`, `payment_type` 등의 컬럼도 존재하나 이번 분석에서는 사용하지 않음.)

### 4.2 HVFHV (`fhvhv_tripdata_*.parquet`)
Yellow와 컬럼 체계가 달라 아래처럼 **공통 스키마로 매핑**하여 병합한다.

| HVFHV 컬럼 | 설명 | Yellow 대응 컬럼 |
|---|---|---|
| `hvfhs_license_num` | 사업자 라이선스 번호 (HV0002~HV0005) | (사업자 구분용, Yellow엔 없음) |
| `pickup_datetime` | 승차 시각 | `tpep_pickup_datetime` |
| `dropoff_datetime` | 하차 시각 | `tpep_dropoff_datetime` |
| `PULocationID` | 승차 Zone ID | `PULocationID` |
| `DOLocationID` | 하차 Zone ID | `DOLocationID` |
| `trip_miles` | 주행 거리 (mile) | `trip_distance` |
| `base_passenger_fare` | 기본 승객 요금 (USD) | `fare_amount` |

> 통합 스키마: `service_type`, `operator`, `pickup_datetime`, `pu_location_id`, `do_location_id`, `trip_distance`, `fare_amount`

### 4.3 taxi_zone_lookup.csv

| 컬럼 | 설명 |
|---|---|
| `LocationID` | Zone 고유 ID (1~265) |
| `Borough` | 자치구 (Manhattan, Queens, Brooklyn, Bronx, Staten Island, EWR) |
| `Zone` | Zone 이름 (예: JFK Airport, Times Sq/Theatre District) |
| `service_zone` | 서비스 구역 구분 (Airports, Boro Zone, Yellow Zone 등) |

### 4.4 MTA GTFS Static (`gtfs_*/*.txt`)
표준 GTFS 포맷 중 이 프로젝트에서 사용하는 파일:

| 파일 | 주요 컬럼 | 용도 |
|---|---|---|
| `routes.txt` | `route_id`, `route_short_name`, `route_long_name`, `agency_id` | 노선 메타(이름/운영사) |
| `trips.txt` | `trip_id`, `route_id`, `shape_id`, `direction_id`, `service_id` | 노선↔경로(shape)↔운행 연결 |
| `shapes.txt` | `shape_id`, `shape_pt_lat`, `shape_pt_lon`, `shape_pt_sequence` | 노선 실제 주행 경로(위경도 점열) |
| `stop_times.txt` | `trip_id`, `stop_sequence`, `departure_time` | 배차 간격(headway) 계산 |
| `calendar.txt` | `service_id`, `monday`~`sunday` | 평일 대표 운행 스케줄 선별 |
| `agency.txt` | `agency_id`, `agency_name` | 운영사 이름 매핑 |

---

## 5. 데이터 필터링 / 샘플링

### 5.1 택시 데이터 — JFK 출발 트립만 추출
- `PULocationID == 132` (JFK Airport Zone)인 트립만 필터링.
- Yellow + HVFHV를 공통 스키마로 병합 후, 결측/논리오류(요금·거리 ≤ 0) 제거, service_type별 상·하위 1% 이상치 컷오프.
- 목적지(`DOLocationID`) Zone 단위로 집계 → `output/jfk_zone_stats/jfk_zone_stats.csv`

  집계 컬럼: `do_location_id, zone, borough, trip_count, avg_distance_mi, total_distance_mi, avg_fare, total_fare, fare_median`

  | do_location_id | zone | borough | trip_count | avg_fare |
  |---|---|---|---|---|
  | 265 | Outside of NYC | N/A | 354,243 | 122.96 |
  | 230 | Times Sq/Theatre District | Manhattan | 67,707 | 87.40 |
  | 164 | Midtown South | Manhattan | 40,269 | 83.24 |

  → JFK에서 어느 Zone으로 얼마나 많이, 얼마의 요금으로 이동하는지가 한눈에 드러난다. (관련 스크립트: `jobs/jfk_zone_stats.py`)

### 5.2 버스 데이터 — JFK를 지나는 노선만 추출
- 각 노선의 `shapes.txt` 경로(LineString)가 **JFK Zone 폴리곤(LocationID=132)과 교차**하는지를 공간 조인으로 판정.
- 교차하는 노선만 JFK 경유 노선으로 자동 탐지 → 총 **8개 노선**: `B15, Q3, Q6, Q7, Q10, Q80, Q113, Q114`
- (관련 스크립트: `jobs/nyc_bus_route.py`)

---

## 6. 버스 노선 데이터 가공

택시 데이터의 출발지·목적지는 **Zone 단위**로만 주어진다. 따라서 버스 노선도 "이 노선이 **어떤 Zone들을 지나가는지**"로 변환해야 택시 수요와 대조할 수 있다.

**가공 방식**
- 각 노선의 주행 경로(`shapes.txt`의 위경도 점열)를 LineString으로 만들고,
- taxi_zones 폴리곤과 공간 조인(intersects)하여, 그 노선이 통과하는 모든 Zone 목록을 산출,
- 동시에 `stop_times.txt`의 출발 시각에서 시간대별 배차 간격(headway, 분)을 계산.
- 결과를 노선별 JSON으로 묶었다.

**결과 파일**: [`output/bus_data/route_zone_coverage.json`](output/bus_data/route_zone_coverage.json)

**스키마 (노선 1개당)**
```json
{
  "route_id": "B15",
  "route_name": "B15",
  "route_long_name": "Bedford Stuyvesant - JFK AirTrain",
  "agency": "MTA New York City Transit",
  "gtfs_source": "gtfs_b",
  "covered_zones": [
    { "location_id": 132, "zone": "JFK Airport", "borough": "Queens" },
    { "location_id": 124, "zone": "Howard Beach", "borough": "Queens" }
  ],
  "headway_min": {
    "peak": 4.5,
    "offpeak": 5.2,
    "by_hour": { "0": 15.0, "8": 5.0, "17": 4.3 }
  }
}
```

| 필드 | 설명 |
|---|---|
| `route_name` / `route_long_name` | 노선 번호 / 노선 전체 이름 |
| `agency` | 실제 운영사 (예: MTA Bus Company) |
| `gtfs_source` | 원본 GTFS 피드 출처 (gtfs_q, gtfs_b 등) |
| `covered_zones` | **이 노선이 지나는 Zone 목록** (택시 목적지 Zone과 매칭하는 핵심 필드) |
| `headway_min.peak` / `offpeak` | 러시아워(07–09, 16–19시) / 그 외 시간대 평균 배차 간격(분) |
| `headway_min.by_hour` | 0~23시 시간대별 배차 간격(분) |

이로써 **택시 목적지 Zone별 수요(5.1)** 와 **버스 노선이 커버하는 Zone·배차(6)** 를 Zone ID를 공통 키로 대조할 수 있는 준비가 끝났다.

---
