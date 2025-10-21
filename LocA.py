##################################
import NearestFinder # YH
import os, json, time, random, warnings, re, requests
import pandas as pd
from tqdm import tqdm
from typing import Optional, Tuple, Dict, Iterable, Union
##################################
import snapCoords # JW
import pandas as pd 
import time
import os
import json
import math
import re
from tqdm import tqdm
##################################
import routeExtract # SY
import pandas as pd
import time
import os
import json
import warnings
from tqdm import tqdm
from shapely.geometry import Point
import numpy as np
##################################

class LocA:
    def __init__(self, INPUT_FILE, KAKAO_API_KEY):
        self.INPUT_FILE = INPUT_FILE
        self.KAKAO_API_KEY = KAKAO_API_KEY
        self.dep = None
        self.acc_coord = None
        self.dst = None
        self.dep_major_address = None
        self.dep_full_address = None
        self.dep_coord = None
        self.acc_full_address = None
        self.acc_coord = None
        self.dst_full_address = None
        self.dst_full_address = None 
        self.dst_coord = None
        self.snap_dep_coord = None
        self.snap_acc_coord = None
        self.snap_dst_coord = None
        self.dep_acc_route = None
        self.acc_dst_route = None
        self.total_route = None

    def get_InputFile(self):
        if all(v is None for v in (self.dep, self.acc_coord, self.dst)):
            print("FileNotFoundError : Input data not initialized.")
            raise ValueError("Age cannot be negative.")
        return self.dep, self.acc_coord, self.dst
    
    def get_NearestCoords(self):
        if all(v is None for v in (self.dep_full_address, self.dep_coord, self.acc_full_address, self.dst_full_address, self.dst_coord)):
            print("AttributeError : \"nearest_coords\" is not running.")
        return self.dep_full_address, self.dep_coord, self.acc_full_address, self.dst_full_address, self.dst_coord
    
    def get_SnappedCoords(self):
        if all(v is None for v in (self.snap_dep_coord, self.snap_acc_coord, self.snap_dst_coord)):
            print("AttributeError : \Snapper\" is not running.")
        return self.snap_dep_coord, self.snap_acc_coord, self.snap_dst_coord
    
    def get_RouteCoords(self):
        if all(v is None for v in (self.dep_acc_route, self.acc_dst_route, self.total_route)):
            print("AttributeError : \"route_extractor\" is not running.")
        return self.dep_acc_route, self.acc_dst_route, self.total_route

    def Series_2_coords(self, coord_series: pd.Series, 
                        return_type: str = 'tuple'):
        df_coords = coord_series.str.split(',', expand=True)

        if 0 not in df_coords.columns:
            df_coords[0] = np.nan
        if 1 not in df_coords.columns:
            df_coords[1] = np.nan

        df_coords.rename(columns={0: 'latitude', 1: 'longitude'}, inplace=True)

        df_coords['latitude'] = pd.to_numeric(df_coords['latitude'], errors='coerce')
        df_coords['longitude'] = pd.to_numeric(df_coords['longitude'], errors='coerce')

        print(df_coords.head())

        if return_type == 'dataframe':
            return df_coords
        
        lat_series = df_coords['latitude']
        lon_series = df_coords['longitude']
        
        return lat_series, lon_series

    
    def coords_2_Series(self, val) -> Optional[Tuple[float, float]]:
        FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

        if val is None: return None
        nums = FLOAT_RE.findall(str(val))
        if len(nums) < 2: return None
        try:
            lat, lon = float(nums[0]), float(nums[1])
            if not (-90 <= lat <= 90 and -180 <= lon <= 180): return None
            return lat, lon
        except:
            return None
        
    def nearest_coords(self, limit = None):
        finder = NearestFinder.NearestFind(self.KAKAO_API_KEY)
        try:
            input_file, df = finder.run_pipeline(
                self.INPUT_FILE, 
                "routes_via_accident_resolved.xlsx", 
                test_limit = limit 
            )
            self.dep = input_file['dep']
            self.acc_coord = input_file['acc_coord']
            self.dst = input_file['dst']
            self.dep_major_address = df["dep_major_address"]
            self.dep_full_address = df["dep_full_address"]
            self.dep_coord = df['dep_coord']
            self.acc_full_address = df['acc_full_address']
            self.acc_coord = df["acc_coord"]
            self.dst_major_address = df["dst_major_address"]
            self.dst_full_address = df["dst_full_address"]
            self.dst_coord = df["dst_coord"]

        except Exception as e:
            df = None
            print(f"\n❌ An error occurred during pipeline execution.: {e}")
            print(df)
        return df
    def Snapper(self):
        snapper =  snapCoords.SNAP(self.KAKAO_API_KEY)
        data = {
            "dep_full_address" : self.dep_full_address,
            'dep_coord' : self.dep_coord,
            'acc_full_address' : self.acc_full_address,
            "acc_coord" : self.acc_coord,
            "dst_full_address" : self.dst_full_address,
            "dst_coord" : self.dst_coord
        }
        df = pd.DataFrame(data)
        self.snap_dep_coord, self.snap_acc_coord, self.snap_dst_coord = snapper.run(df)
        return self.snap_dep_coord, self.snap_acc_coord, self.snap_dst_coord
    ##################################
    def route_extractor(self):

        snap_dep_lat, snap_dep_lon = self.Series_2_coords(self.snap_dep_coord)
        snap_acc_lat, snap_acc_lon = self.Series_2_coords(self.snap_acc_coord)
        snap_dst_lat, snap_dst_lon = self.Series_2_coords(self.snap_dst_coord)


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

        Extract = routeExtract.Extractor(self.KAKAO_API_KEY)

        print("##### Extracting the route from the origin to the accident location #####")
        res_dep_acc_df = Extract.process_routes_from_dataframe(dep_acc_df)
        res_dep_acc_df.columns = ['lat', 'lon', 'dest_lat', 'dest_lon', "snap_dep_coord", "snap_acc_coord", "dep_acc_route", "src"]
        
        
        print("##### Extracting the route from the accident location to the Destination #####")
        res_acc_dst_df = Extract.process_routes_from_dataframe(acc_dst_df)
        res_acc_dst_df.columns = ['lat', 'lon', 'dest_lat', 'dest_lon', "snap_acc_coord", "snap_dst_coord", "acc_dst_route", "src"]
        
        result_df = pd.concat([res_dep_acc_df['dep_acc_route'], res_acc_dst_df['acc_dst_route']], axis = 1)
        result_df['total_route'] = (
                                    result_df['dep_acc_route'].fillna('').astype(str) + 
                                    '; ' + 
                                    result_df['acc_dst_route'].fillna('').astype(str)
                                    )
        print("--- total_route --- ")
        print(result_df.head())
        self.dep_acc_route = result_df["dep_acc_route"]
        self.acc_dst_route = result_df["acc_dst_route"]
        self.total_route = result_df["total_route"]

    def save_file(self, OUTPUT_FILE):
        data = {
            "dep" : self.dep, 
            "acc_coord" :  self.acc_coord,
            "dst" : self.dst,
            "dep_major_address" : self.dep_major_address,
            "dep_full_address" : self.dep_full_address,
            "dep_coord" : self.dep_coord ,
            "acc_full_addresss" : self.acc_full_address,
            "acc_coord" : self.acc_coord,
            "dst_major_address" : self.dst_major_address,
            "dst_full_address" : self.dst_full_address,
            "dst_coord" : self.dst_coord,
            "snap_dep_coord" :  self.snap_dep_coord,
            "snap_acc_coord" :  self.snap_acc_coord,
            "snap_dst_coord" :  self.snap_dst_coord,
            "dep_acc_route" : self.dep_acc_route ,
            "acc_dst_route" : self.acc_dst_route,
            "total_route" : self.total_route,     
        }
        dataFrame = pd.DataFrame(data)
        dataFrame.to_excel(OUTPUT_FILE)
        print("==== Save Finish ====")


if __name__ == "__main__":
    INPUT_FILE = "Samples/initial_input_data.xlsx"
    OUTPUT_FILE = "./result/total.xlsx"
    KAKAO_API_KEY = 'Input Your API KEY'
    LocA_run = LocA(INPUT_FILE,KAKAO_API_KEY) 
    df = LocA_run.nearest_coords() # YH
    LocA_run.Snapper() # JW
    LocA_run.route_extractor() # SY
    LocA_run.save_file(OUTPUT_FILE)