import os, json, time, random, warnings, re, requests
import pandas as pd
from tqdm import tqdm
from typing import Optional, Tuple, Dict, Iterable, Union

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")
RADIUS_STEPS = (3000, 10000, 20000)
API_DELAY = 0.08


class NearestFind:
    def __init__(self, kakao_api_key: str, cache_path: str = "nearest_cache.json"):
        self.session = requests.Session()
        self.headers = {"Authorization": f"KakaoAK {kakao_api_key}"}
        
        self.cache_path = cache_path
        self.cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def _http_get_with_retry(self, url, headers, params, timeout=8, max_retries=3):
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(0.3 * (2 ** attempt))
                    continue
                r.raise_for_status()
                return r
            except requests.exceptions.RequestException:
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.3 * (2 ** attempt))

    def _kakao_keyword_nearest(self, query, center_lat, center_lon, radius) -> Optional[Tuple[float, float, str, str]]:
        if not query or str(query).strip() == "":
            return None
        params = {"query": str(query).strip(), "y": center_lat, "x": center_lon,
                  "radius": radius, "sort": "distance", "page": 1, "size": 1}
        time.sleep(API_DELAY)
        try:
            data = self._http_get_with_retry(
                "https://dapi.kakao.com/v2/local/search/keyword.json", 
                self.headers, params
            ).json()
        except requests.exceptions.RequestException:
            return None
            
        docs = data.get("documents") or []
        if not docs: return None
        d = docs[0]
        return (float(d["y"]), float(d["x"]), d.get("place_name"), d.get("road_address_name") or d.get("address_name"))

    def _kakao_reverse_geocode_fulladdr(self, lat, lon) -> Optional[str]:
        try:
            params = {"y": lat, "x": lon}
            time.sleep(API_DELAY)
            data = self._http_get_with_retry(
                "https://dapi.kakao.com/v2/local/geo/coord2address.json", 
                self.headers, params
            ).json()
            docs = data.get("documents") or []
            if not docs: return None
            addr = docs[0].get("road_address") or docs[0].get("address") or {}
            return addr.get("address_name")
        except Exception:
            return None

    @staticmethod
    def _parse_coord_series(val) -> Optional[Tuple[float, float]]:
        if val is None: return None
        nums = FLOAT_RE.findall(str(val))
        if len(nums) < 2: return None
        try:
            lat, lon = float(nums[0]), float(nums[1])
            if not (-90 <= lat <= 90 and -180 <= lon <= 180): return None
            return lat, lon
        except:
            return None

    def _find_nearest_by_steps(self, query, acc_lat, acc_lon) -> Optional[Tuple[float, float, str, str]]:
        for r in RADIUS_STEPS:
            key = f"{query}-{acc_lat}-{acc_lon}-{r}"
            if key in self.cache: return self.cache[key]
            
            res = self._kakao_keyword_nearest(query, acc_lat, acc_lon, r)
            
            if res:
                self.cache[key] = res
                return res
        return None

    def process_row(self, row: pd.Series) -> Dict:
        dep_q, dst_q = row.get("dep"), row.get("dst")
        acc_pair = self._parse_coord_series(row.get("acc_coord"))
        
        out = {"dep_full_address": None, "dep_coord": None,
               "acc_full_address": None, "acc_coord": None,
               "dst_full_address": None, "dst_coord": None}
        
        if not acc_pair: return out

        acc_lat, acc_lon = acc_pair
        
        out["acc_full_address"] = self._kakao_reverse_geocode_fulladdr(acc_lat, acc_lon)
        out["acc_coord"] = (acc_lat, acc_lon) 

        dep_is_none = pd.isna(dep_q) or str(dep_q).strip() == ""
        dst_is_none = pd.isna(dst_q) or str(dst_q).strip() == ""

        if not dst_is_none:
            nd = self._find_nearest_by_steps(dst_q, acc_lat, acc_lon)
            if nd:
                d_lat, d_lon, place, addr = nd
                full = self._kakao_reverse_geocode_fulladdr(d_lat, d_lon) or addr
                if place: full = f"{place} ({full})" if full else place
                out["dst_full_address"] = full
                out["dst_coord"] = (d_lat, d_lon) 
        
        if not dep_is_none:
            ns = self._find_nearest_by_steps(dep_q, acc_lat, acc_lon)
            if ns:
                s_lat, s_lon, place, addr = ns
                full = self._kakao_reverse_geocode_fulladdr(s_lat, s_lon) or addr
                if place: full = f"{place} ({full})" if full else place
                out["dep_full_address"] = full
                out["dep_coord"] = (s_lat, s_lon) 

        return out

    def run_pipeline(self, input_excel: str, output_excel: str, test_limit: Optional[int] = None):
        try:
            df = pd.read_excel(input_excel)
        except FileNotFoundError:
            print(f"❌ Error> Input file '{input_excel}' could not be found.")
            return

        if test_limit: 
            df = df.head(test_limit)
            print(f"⚠️ Test mode: Processing only the top {test_limit} rows.")
        
        out = pd.DataFrame(columns=[
            "dep_full_address", "dep_coord", "acc_full_address", 
            "acc_coord", "dst_full_address", "dst_coord"
        ])
        print(f"Starting data processing. Total {len(df)} rows...")
        
        for idx, row in tqdm(df.iterrows(), total=len(df), ncols=90, desc="Resolving"):
            res = self.process_row(row)
            out.loc[idx] = res

        self._save_cache()
        out.to_excel(output_excel, index=False)
        
        print(f"✅ Processing complete and saved: {output_excel}")
        return  df, out

