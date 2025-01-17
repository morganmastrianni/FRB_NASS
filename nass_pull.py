import requests as req
import os
from dotenv import load_dotenv
# import json

load_dotenv()
base_url = "https://quickstats.nass.usda.gov/api/"
api_key = os.getenv("NASS_api_key")
rsc = "get_param_values"

# def get_nass_quickstats(frb_dist, rsc, params):
#     url = f"{base_url}/{rsc}/?key={api_key}&{params}"
#     req.get(url).text

# frb = "kc"
state = "NE"
# get_nass_quickstats(frb, income)

data = req.get(
    f"{base_url}/{rsc}/?key={api_key}&param=county_code&state_alpha={state}"
).text
print(data)


