import requests as req

base_url = "https://quickstats.nass.usda.gov/api/"
api_key = "4719014D-12BE-3B3D-9339-6C9F991F153C"
rsc = "get_counts"

# def get_nass_quickstats(frb_dist, rsc, params):
#     url = f"{base_url}/{rsc}/?key={api_key}&{params}"
#     req.get(url).text

# frb = "kc"
# get_nass_quickstats(frb, income)

data = req.get(f"{base_url}/{rsc}/?key={api_key}&commodity_desc=CORN&year__GE=2012&state_alpha=N").text
print(data)