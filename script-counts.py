import pandas as pd

raw = pd.read_csv("data/max temp (°c)-dayssincerecord.csv").set_index("Unnamed: 0")

df = (raw.iloc[:, 1:] == 0).sum(axis=1)
df.index = df.index.map(lambda x: pd.to_datetime(x))
df = df.resample("1Y").sum()
df.index = df.index.strftime("%Y")

df.to_csv("data/recordcounts.csv")

df.to_clipboard()