import pandas as pd
from datawrapper import Datawrapper
import os
import datetime as dt

now = dt.datetime.today()
day = now.strftime('%B %d').replace(" 0", " ")
date = now.strftime('%B %d, %Y').replace(" 0", " ")
time = now.strftime('%I:%M') + " " + ".".join(list(now.strftime('%p'))).lower() + "."

yesterday = dt.datetime.today() - dt.timedelta(days=1)
yesterday_date = yesterday.strftime('%B %d, %Y').replace(" 0", " ")

try:
    with open('./auth.txt', 'r') as f:
        DW_AUTH_TOKEN = f.read().strip()    
except:
    DW_AUTH_TOKEN = os.environ['DW_AUTH_TOKEN']

for metric in ["hot", "cold"]:
    if metric == "hot":
        raw = pd.read_csv("data/max temp (°c)-dayssincerecord.csv")
    elif metric == "cold":
        raw = pd.read_csv("data/min temp (°c)-dayssincerecord.csv")
        
    raw = raw.dropna()

    today = raw.tail(1).transpose().drop(["Unnamed: 0"])
    today.columns = ["Days since record"]
    today["Days since record"] = today["Days since record"].astype(int)
    today = today.sort_values("Days since record", ascending=False).reset_index()
    today["Rank"] = range(1,len(today)+1)
    today = today[["Rank", "index", "Days since record"]]

    print(today)

    dw = Datawrapper(access_token=DW_AUTH_TOKEN)

    metadata_update = {
                "annotate": {
                    "notes": f"Last updated on {date} at {time} Temperature data for each city is based on readings at airport weather stations. Some weather stations have not been operational every day going back to 1980.".replace(" 0", " ")
                }
            }
    
    if metric == "hot":
        CHART_ID = "Zypiu"
        description = f"Every day, we compare that day's max temperature to the same date going back to 1980 to see if it was the <span style='color:#FF7F00'><b>🔥hottest</b></span>. Below, you can see how many days it's been since each city has recorded a record high temperature.<br><br>As of yesterday (<b>{yesterday_date}</b>), <b>{today.at[0, 'index']}</b> had gone the longest without breaking a <span style='color:#FF7F00'><b>🔥heat</b></span> record (<b>{today.at[0, 'Days since record']} day{'s' if today.at[0, 'Days since record'] != 0 else ''}</b>).<br><br>Find your city using the search box below, or scroll through rankings using the arrow buttons."
    elif metric == "cold":
        CHART_ID = "bulmP"
        description = f"Every day, we compare that day's minimum temperature to the same date going back to 1980 to see if it was the <span style='color:#1F78B4'><b>❄️coldest</b></span>. Below, you can see how many days it's been since each city has recorded a record low temperature.<br><br>As of yesterday (<b>{yesterday_date}</b>), <b>{today.at[0, 'index']}</b> had gone the longest without breaking a <span style='color:#1F78B4'><b>❄️cold</b></span> record (<b>{today.at[0, 'Days since record']} day{'s' if today.at[0, 'Days since record'] != 0 else ''}</b>).<br><br>Find your city using the search box below, or scroll through rankings using the arrow buttons."

    dw.update_metadata(chart_id=CHART_ID, properties=metadata_update)
    dw.publish_chart(chart_id=CHART_ID)
    dw.update_description(chart_id=CHART_ID, intro=description)

    dw.add_data(chart_id=CHART_ID, data=today)
    dw.export_chart(chart_id=CHART_ID, filepath=f"images/{metric}-{date.lower()}.png", width=600)