import pandas as pd

def transform_mutual_fund_df(fund_df):
    if fund_df is None or fund_df.empty:
        return

    print(fund_df.head(20))
    fund_df["date"] = pd.to_datetime(fund_df["date"], format='%d-%m-%Y')
    fund_df["nav"] = fund_df["nav"].astype(float)
    fund_df.set_index('date', inplace=True)
    print(fund_df.head(20))
    return fund_df