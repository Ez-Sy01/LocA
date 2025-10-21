import pandas as pd
import requests
import time
import os
import json
import warnings
from tqdm import tqdm
from shapely.geometry import Point
import osmnx as ox

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

ox.settings.use_cache = True
ox.settings.log_console = False
class Extractor:
    def __init__(self, KAKAOAPI_KEY, API_DELAY = 0.05):
        self.GEOCODE_CACHE_FILE = 'geocode_cache.json'
        self.KAKAO_API_KEY = KAKAOAPI_KEY
        self.API_DELAY = API_DELAY

    # --- 카카오 길찾기 함수
    def get_route(self, start_lat, start_lon, end_lat, end_lon):
        url = "https://apis-navi.kakaomobility.com/v1/directions"
        headers = {"Authorization": f"KakaoAK {self.KAKAO_API_KEY}"}
        params = {"origin": f"{start_lon},{start_lat}", "destination": f"{end_lon},{end_lat}"}
        
        try:
            time.sleep(self.API_DELAY)
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()

            if "routes" in result and result["routes"] and "sections" in result["routes"][0] and result["routes"][0]["sections"]:
                route = result["routes"][0]
                all_vertexes = []
                for section in route["sections"]:
                    for road in section["roads"]:
                        vertex_list = road["vertexes"]
                        path_coords = [(vertex_list[i], vertex_list[i+1]) for i in range(0, len(vertex_list), 2)]
                        all_vertexes.extend(path_coords)
                
                if all_vertexes:
                    unique_path = [all_vertexes[i] for i in range(len(all_vertexes)) if i == 0 or all_vertexes[i] != all_vertexes[i-1]]
                    
                    start_node = f"({unique_path[0][1]:.6f}, {unique_path[0][0]:.6f})"
                    end_node = f"({unique_path[-1][1]:.6f}, {unique_path[-1][0]:.6f})"
                    path_str = "; ".join([f"({lat:.6f}, {lon:.6f})" for lon, lat in unique_path])
                    
                    return {"start_node": start_node, "end_node": end_node, "path": path_str, "source": "Kakao"}

        except requests.exceptions.RequestException:
            pass

        return None

    def process_routes_from_dataframe(self, input_df: pd.DataFrame) -> pd.DataFrame:      
        print("--- Accident path node extraction initialization (INPUT DataFrame) ---")
        df = input_df.copy()
        required_cols = ['lat', 'lon', 'dest_lat', 'dest_lon']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"The input DataFrame must contain the following columns: {required_cols}")

        df['사고지점 노드 좌표(path)'] = None
        df['목적지 노드 좌표(path)'] = None
        df['노드 경로(path)'] = None
        df['경로 탐색 소스'] = None

        df_processed = df[['lat', 'lon', 'dest_lat', 'dest_lon']].copy()
        df_processed.columns = ['start_lat', 'start_lon', 'dest_lat', 'dest_lon']

        df_processed.dropna(subset=['start_lat', 'start_lon', 'dest_lat', 'dest_lon'], inplace=True)
        if df_processed.empty:
            print("❌ There is no data with valid start/destination coordinates. Route extraction will be skipped.")
            return df

        print(f"✅ Start extracting a total of {len(df_processed)} paths")

        print("\n--- Path node extraction Start... (KaKao API)")
        
        results_dict = {}
        for idx, row in tqdm(df_processed.iterrows(), total=df_processed.shape[0], desc="Path extraction progress"):
            route_info = self.get_route(
                row['start_lat'], row['start_lon'], 
                row['dest_lat'], row['dest_lon']
            )
            results_dict[idx] = route_info

        # 결과 업데이트
        for idx, result in results_dict.items():
            if result:
                df.loc[idx, '사고지점 노드 좌표(path)'] = result.get('start_node')
                df.loc[idx, '목적지 노드 좌표(path)'] = result.get('end_node')
                df.loc[idx, '노드 경로(path)'] = result.get('path')
                df.loc[idx, '경로 탐색 소스'] = result.get('source')

        print("\n--- Path node extraction Complete! ---")
        print(f"Extracted {df['노드 경로(path)'].notna().sum()} paths out of {len(df)} total records.")
        
        return df

# --- 실행 예제 ---
if __name__ == '__main__':

    snap_dep_coord = pd.Series([(37.5665, 126.9780), (37.5500, 126.9900), (37.5800, 127.0050), (37.5400, 126.9800)])
    snap_acc_coord = pd.Series([(37.5511, 126.9903), (37.5514, 126.9880), (37.5976, 126.9764), (None, None)])
    snap_dst_coord = pd.Series([(37.5574, 126.9254), (37.5126, 127.1028), (37.5273, 126.9185), (None, None)])
    
    # data decomposition
    snap_dep_lat = snap_dep_coord.apply(lambda x: x[0])
    snap_dep_lon = snap_dep_coord.apply(lambda x: x[1])
    snap_acc_lat = snap_acc_coord.apply(lambda x: x[0])
    snap_acc_lon = snap_acc_coord.apply(lambda x: x[1])
    snap_dst_lat = snap_dst_coord.apply(lambda x: x[0])
    snap_dst_lon = snap_dst_coord.apply(lambda x: x[1])

    dep_acc_data = {
        'lat': snap_dep_lat,  
        'lon': snap_dep_lon,
        'dest_lat': snap_acc_lat, 
        'dest_lon': snap_acc_lon 
    }
    acc_dst_data = {
        'lat': snap_acc_lat,  
        'lon': snap_acc_lon, 
        'dest_lat': snap_dst_lat,
        'dest_lon': snap_dst_lon
    }

    dep_acc_df = pd.DataFrame(dep_acc_data)
    acc_dst_df = pd.DataFrame(acc_dst_data)

    # 함수 실행
    KAKAO_API_KEY = "b18faf1d0177c585e56fc757bc4687f2"
    Extract = Extractor(KAKAO_API_KEY)
    res_dep_acc_df = Extract.process_routes_from_dataframe(dep_acc_df)
    res_dep_acc_df.columns = ['lat', 'lon', 'dest_lat', 'dest_lon', "snap_dep_coord", "snap_acc_coord", "dep_acc_route", "src"]
    res_acc_dst_df = Extract.process_routes_from_dataframe(acc_dst_df)
    res_acc_dst_df.columns = ['lat', 'lon', 'dest_lat', 'dest_lon', "snap_acc_coord", "snap_dst_coord", "acc_dst_route", "src"]
    
    result_df = pd.concat([res_dep_acc_df['dep_acc_route'], res_acc_dst_df['acc_dst_route']], axis = 1)
    result_df['total_route'] = (
                                result_df['dep_acc_route'].fillna('').astype(str) + 
                                '; ' + 
                                result_df['acc_dst_route'].fillna('').astype(str)
                                )
    print("(total_route)")
    print(result_df)

    result_df.to_excel('결과_route_data_coords_input_lat_lon.xlsx', index=False)