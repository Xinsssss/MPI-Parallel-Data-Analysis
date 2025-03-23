# from mpi4py import MPI
from datetime import datetime
import pandas as pd

START_TIME = datetime.now()

df = pd.read_json("mastodon-106k.ndjson", lines=True)

doc_df = pd.json_normalize(df["doc"])

useful_df = doc_df[["createdAt","sentiment","account.id","account.username"]]

print(useful_df.head(3))

END_TIME = datetime.now()

print("Running time: " + str(END_TIME - START_TIME))

