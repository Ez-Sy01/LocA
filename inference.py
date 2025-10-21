import LocA
import pandas as pd

INPUT_FILE = "Samples/initial_input_data.xlsx"
OUTPUT_FILE = "./result/total_inference.xlsx"
KAKAO_API_KEY = "INPUT YOUR API KEY" 
LocA_run = LocA.LocA(INPUT_FILE,KAKAO_API_KEY) 
df = LocA_run.nearest_coords() # YH
LocA_run.Snapper() # JW
LocA_run.route_extractor() # SY
LocA_run.save_file(OUTPUT_FILE)