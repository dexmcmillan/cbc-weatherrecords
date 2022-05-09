import pandas as pd
import datetime as dt
from datetime import timedelta
import string

airports_list = pd.read_csv("airport_ids.csv").index.to_list()

li = []

today = dt.datetime.today()
day = today.day
month = today.month
year = today.year

for station_id in airports_list:
    df = pd.read_csv(f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={str(station_id)}&Year={year}&timeframe=2')
    df = df.loc[(df["Month"] == month) & (df["Day"] == day),:]
    df.insert(0, "Station ID", station_id)
    li.append(df)
    
additions = pd.concat(li)

raw = pd.read_csv("data/data-historical.csv")
raw["Date/Time"] = pd.to_datetime(raw["Date/Time"])
raw = raw[raw["Date/Time"] < today]
raw = pd.concat([raw, additions]).drop_duplicates(subset=["Station ID", "Date/Time"])
raw["Climate ID"] = raw["Climate ID"].astype(str)

raw.to_csv("data/data-raw.csv")

start_date = "2017-01-01"

mydates = pd.date_range(start_date, today)
mydates = pd.to_datetime(mydates.date).astype(str).to_list()

days_list = []

for metric in ["Min Temp (°C)", "Max Temp (°C)"]:
    
    previous_data = pd.read_csv(f"data/{metric.lower()}-dayssincerecord.csv")
    
    dates = previous_data.loc[previous_data["Unnamed: 0"] >= start_date, "Unnamed: 0"].to_list()
    unique_dates = [elem for elem in dates if elem not in mydates ]
    
    if len(unique_dates) > 0:

        for date in unique_dates:
            
            print(f"Analyzing date: {date}")
            
            data = raw[pd.to_datetime(raw["Date/Time"]) <= pd.to_datetime(date)]

            lis_max = []

            for climate_id in data["Climate ID"].astype(str).unique():
                
                station_data = (data[data["Climate ID"] == climate_id]
                                .pivot(columns=["Climate ID", "Station Name", "Month", "Day"], index="Year", values=metric)
                                .dropna(how="all", axis=1)
                                )
                
                if metric == "Max Temp (°C)":
                    max = pd.DataFrame(station_data.idxmax()).reset_index().rename(columns={0: "Year"})
                elif metric == "Min Temp (°C)":
                    max = pd.DataFrame(station_data.idxmin()).reset_index().rename(columns={0: "Year"})
                
                max["date"] = pd.to_datetime(max[["Year", "Month", "Day"]])
                max["days_since_record"] = -(max["date"] - pd.to_datetime(date)).dt.days

                max = max[["Station Name", "date", "days_since_record"]].set_index("date")
                
                lis_max.append(max)
                
            df = pd.concat(lis_max).reset_index()

            max_values = df.pivot_table(index="Station Name", values=["days_since_record"], aggfunc="min").sort_values("days_since_record")
            max_values["date"] = max_values["days_since_record"].apply(lambda x: pd.to_datetime(date) - timedelta(days=x)).astype(str).str.slice(0, 10)

            locations = (data
                        .loc[:, ["Station Name", "Latitude (y)", "Longitude (x)"]]
                        .drop_duplicates("Station Name")
                        .set_index("Station Name")
                        )

            final = max_values.join(locations)

            final.index = (final.index
                        .str.replace(" INTL A", "")
                        .str.replace(" INT'L A", "")
                        .str.replace(" INT'L CS", "")
                        .str.replace(" INTERNATIONAL CS", "")
                        .str.replace(" STANFIELD", "")
                        .str.replace("/GREATER MONCTON ROMEO LEBLANC", "")
                        )
            
            final.index = final.index.map(lambda x: string.capwords(x))
            
            final = final[["days_since_record"]]
            final["days_since_record"] = final["days_since_record"].astype(str)
            final.columns = [date]
            
            days_list.append(final)
            
        new_days = pd.concat(days_list, axis=1).transpose()
        
        export = pd.concat([previous_data, new_days])

        print(export)

        export.to_csv(f"data/{metric.lower()}-dayssincerecord.csv")

    else:
        print(f"No data to fetch for {metric}")