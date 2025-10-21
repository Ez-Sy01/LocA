import pandas as pd
import requests
import time
import os
import json
import math
import re
from tqdm import tqdm
class SNAP:
    def __init__(self, KAKAO_API_KEY):
        self.SNAP_CACHE_FILE = 'snap_cache_kakao_only.json'        
        self.KAKAO_API_KEY = KAKAO_API_KEY    
        self.API_DELAY = 0.05      
                                        
        self.SESSION = requests.Session()
        self.HEADERS = {"Authorization": f"KakaoAK {self.KAKAO_API_KEY}"}

        self.DEDUP_ROUND = 4                 
        self.FAST_OFFSET_M = 120             
        self.MAX_SNAP_METERS = 200           
        self.EARLY_HIT_METERS = 80           
        self.MAX_CALLS_PER_POINT = 4         

        self.DIRS = [(1,0), (0,1), (-1,0), (0,-1)]

        self.USE_REGION_FILTER = True
        self.REGION_BBOXES = [
            (127.19, 36.19, 127.54, 36.51),
            (126.13, 36.10, 127.70, 37.05),
            (127.20, 36.10, 128.35, 37.35),
        ]

    def fix_latlon_order(self, lat, lon):
        def in_kr(a,b): return 32.0 <= a <= 39.5 and 124.0 <= b <= 132.5
        try:
            lat = float(lat); lon = float(lon)
        except Exception:
            return None
        if in_kr(lat, lon): return (lat, lon)
        if in_kr(lon, lat): return (lon, lat)
        return (lat, lon)

    def parse_latlon_str(self, s):
        if s is None: return None
        nums = re.findall(r'[-+]?\d+(?:\.\d+)?', str(s))
        if len(nums) < 2: return None
        lat, lon = float(nums[0]), float(nums[1])
        return self.fix_latlon_order(lat, lon)

    def fmt_latlon(self, lat, lon, nd=6):
        return f"{lat:.{nd}f},{lon:.{nd}f}"

    def hav_km(self, lat1, lon1, lat2, lon2):
        R = 6371.0088
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dphi = p2 - p1
        dl = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    def offset_latlon(self, lat, lon, east_m=0.0, north_m=0.0):
        dlat = north_m / 111320.0
        dlon = east_m / (111320.0 * max(0.1, math.cos(math.radians(lat))))
        return (lat + dlat, lon + dlon)

    def uniq_key_from_latlon(self, lat, lon, nd):
        nd = self.DEDUP_ROUND
        return f"{round(lat, nd)},{round(lon, nd)}"

    def in_any_bbox(self, lat, lon):
        bboxes = self.REGION_BBOXES
        for left, bottom, right, top in bboxes:
            if (left <= lon <= right) and (bottom <= lat <= top):
                return True
        return False

    def clamp_to_bbox(self, lat, lon, bbox):
        left, bottom, right, top = bbox
        lon_c = min(max(lon, left), right)
        lat_c = min(max(lat, bottom), top)
        return (lat_c, lon_c)

    def clamp_to_nearest_bbox(self, lat, lon, bboxes):
        bboxes = self.REGION_BBOXES
        if self.in_any_bbox(lat, lon, bboxes):
            return (lat, lon)
        best_pt, best_d = None, 1e18
        for bbox in bboxes:
            lat_c, lon_c = self.clamp_to_bbox(lat, lon, bbox)
            d = (lat - lat_c)**2 + (lon - lon_c)**2
            if d < best_d:
                best_d = d; best_pt = (lat_c, lon_c)
        return best_pt

    def ensure_moved(self, lat, lon, lat2, lon2):
        if abs(lat - lat2) < 1e-7 and abs(lon - lon2) < 1e-7:
            lat2, lon2 = self.offset_latlon(lat, lon, east_m=10, north_m=0)
        return lat2, lon2

    def load_snap_cache(self):
        if os.path.exists(self.SNAP_CACHE_FILE):
            try:
                with open(self.SNAP_CACHE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_snap_cache(self, cache):
        with open(self.SNAP_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)



    def _retry_get(self, url, params, tries=3, base_sleep=0.35):
        for i in range(tries):
            try:
                time.sleep(self.API_DELAY)
                r = self.SESSION.get(url, headers=self.HEADERS, params=params, timeout=10)
                if r.status_code == 429:
                    time.sleep(base_sleep * (i+1) + 0.5) 
                    continue
                r.raise_for_status()
                return r
            except requests.exceptions.RequestException:
                time.sleep(base_sleep * (i+1))
        return None

    def kakao_route_roads(self, start_lat, start_lon, end_lat, end_lon):
        """
        카카오 길찾기 요청 → roads 리스트 반환
        각 road: {'name': str, 'vertexes': [(lon,lat), ...]}
        """
        url = "https://apis-navi.kakaomobility.com/v1/directions"
        params = {
            "origin": f"{start_lon:.6f},{start_lat:.6f}",     
            "destination": f"{end_lon:.6f},{end_lat:.6f}",    
        }
        r = self._retry_get(url, params, tries=3)
        if not r:
            return []
        try:
            data = r.json()
        except Exception:
            return []
        routes = data.get("routes") or []
        if not routes:
            return []
        roads_all = []
        for sec in routes[0].get("sections", []):
            for road in sec.get("roads", []):
                v = road.get("vertexes", []) or []
                if not v:
                    continue
                verts = [(v[i], v[i+1]) for i in range(0, len(v), 2)]  # (lon,lat)
                roads_all.append({
                    "name": road.get("name") or "",
                    "vertexes": verts
                })
        return roads_all

    def nearest_vertex(self, lat, lon, roads):
        best_pt, best_d = None, 1e18
        best_name = ""
        for rd in roads:
            name = (rd.get("name") or "").strip()
            for (vx, vy) in rd.get("vertexes", []): 
                d = self.hav_km(lat, lon, vy, vx)
                if d < best_d:
                    best_d = d
                    best_pt = (vy, vx)
                    best_name = name
        return best_pt, best_d, best_name

    def snap_point_kakao_nearby(self, lat, lon):
        if self.USE_REGION_FILTER and not self.in_any_bbox(lat, lon):
            return None

        collected_roads = []
        calls = 0
        early_pt = None
        early_d_m = None
        early_name = ""

        for ex, ny in self.DIRS:
            if calls >= self.MAX_CALLS_PER_POINT:
                break
            lat2, lon2 = self.offset_latlon(lat, lon, east_m=ex*self.FAST_OFFSET_M, north_m=ny*self.FAST_OFFSET_M)
            if self.USE_REGION_FILTER and not self.in_any_bbox(lat2, lon2):
                lat2, lon2 = self.clamp_to_nearest_bbox(lat2, lon2)
            lat2, lon2 = self.ensure_moved(lat, lon, lat2, lon2)

            roads = self.kakao_route_roads(lat, lon, lat2, lon2)
            calls += 1
            if not roads:
                continue

            pt, d_km, name = self.nearest_vertex(lat, lon, roads)
            if pt:
                d_m = d_km * 1000.0
                if d_m <= self.EARLY_HIT_METERS:
                    return (pt[0], pt[1], round(d_m,1), name) 
                if (early_pt is None) or (d_m < early_d_m):
                    early_pt, early_d_m, early_name = pt, d_m, name

            collected_roads.extend(roads)

        if collected_roads:
            pt, d_km, name = self.nearest_vertex(lat, lon, collected_roads)
            if pt:
                d_m = d_km * 1000.0
                if d_m <= self.MAX_SNAP_METERS:
                    return (pt[0], pt[1], round(d_m,1), name)

        if early_pt and early_d_m is not None and early_d_m <= self.MAX_SNAP_METERS:
            return (early_pt[0], early_pt[1], round(early_d_m,1), early_name)

        return None

    def run(self, df):
        print("--- Starting nearby road snapping (Kakao-only, for Chungcheong/Daejeon region) ---")

        df["dep_parsed"] = df["dep_coord"].map(self.parse_latlon_str)
        df["acc_parsed"] = df["acc_coord"].map(self.parse_latlon_str)
        df["dst_parsed"] = df["dst_coord"].map(self.parse_latlon_str)

        def key_of(t):
            if not t: return None
            lat, lon = t
            return self.uniq_key_from_latlon(lat, lon, nd=self.DEDUP_ROUND)

        all_pts = []
        for c in ["dep_parsed","acc_parsed","dst_parsed"]:
            all_pts.extend(df[c].tolist())
        valid_pts = [t for t in all_pts if t]

        if self.USE_REGION_FILTER:
            before = len(valid_pts)
            valid_pts = [t for t in valid_pts if self.in_any_bbox(t[0], t[1])]
            print(f"Coordinates passed region filter: {len(valid_pts)} / Total valid coordinates: {before}")

        uniq = {}
        for t in valid_pts:
            k = key_of(t)
            if k and k not in uniq:
                uniq[k] = t

        print(f"Unique coordinates to snap: {len(uniq)} / Total rows: {len(df)}")

        snap_cache = self.load_snap_cache()

        results = {}
        src_meta = {}
        dist_meta = {}
        road_meta = {}

        processed = 0
        for k, (lat, lon) in tqdm(uniq.items(), total=len(uniq), desc="SNAP :"):
            if k in snap_cache:
                v = snap_cache[k]
                if isinstance(v, dict) and "y" in v and "x" in v:
                    results[k]  = self.fmt_latlon(v["y"], v["x"])
                    src_meta[k] = "kakao"
                    dist_meta[k]= v.get("dist_m","")
                    road_meta[k]= v.get("road","")
                else:
                    results[k]  = ""
                    src_meta[k] = ""
                    dist_meta[k]= ""
                    road_meta[k]= ""
                continue

            snap = self.snap_point_kakao_nearby(lat, lon)
            if snap:
                y, x, dist_m, rname = snap
                results[k] = self.fmt_latlon(y, x)
                src_meta[k] = "kakao"
                dist_meta[k]= dist_m
                road_meta[k]= rname
                snap_cache[k] = {"y": y, "x": x, "dist_m": dist_m, "road": rname}
            else:
                results[k]  = ""
                src_meta[k] = ""
                dist_meta[k]= ""
                road_meta[k]= ""
                snap_cache[k] = None

            processed += 1
            if processed % 300 == 0:   # 중간 체크포인트 저장
                self.save_snap_cache(snap_cache)

        def map_res(t):
            if not t: return ""
            return results.get(key_of(t), "")

        def map_src(t):
            if not t: return ""
            return src_meta.get(key_of(t), "")

        def map_dist(t):
            if not t: return ""
            return dist_meta.get(key_of(t), "")

        def map_road(t):
            if not t: return ""
            return road_meta.get(key_of(t), "")
        
        data = {
            "snap_dep_coord" : df["dep_parsed"].map(map_res),
            "snap_acc_coord" : df["acc_parsed"].map(map_res),
            "snap_dst_coord" : df["dst_parsed"].map(map_res)
        }
        snap_df = pd.DataFrame(data)
        return df["dep_parsed"].map(map_res),df["acc_parsed"].map(map_res), df["dst_parsed"].map(map_res)

if __name__ == "__main__":
    INPUT_FILE = 'routes_via_accident_resolved.xlsx'

    if INPUT_FILE.lower().endswith((".xlsx",".xls")):
        df = pd.read_excel(INPUT_FILE)
    else:
        try:
            df = pd.read_csv(INPUT_FILE, encoding='cp949')
        except Exception:
            df = pd.read_csv(INPUT_FILE, encoding='utf-8')
    df = df.iloc[:5, :]
    print(df)
    for c in ["dep_coord","acc_coord","dst_coord"]:
        if c not in df.columns:
            raise KeyError(f"The column '{c}' could not be found. The current columns are: {list(df.columns)}")
    snapper = SNAP('Input Your API KEY')
    mod_df = snapper.run(df)
    print(mod_df.keys())
    print(mod_df.head())
    