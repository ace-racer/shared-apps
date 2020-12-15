import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List
from datetime import datetime, timedelta
import streamlit as st
import joblib

import src.utils as utils
from src.india_mf_nav_obtainer import IndiaMFNavObtainer

india_mf_nav_obtainer = IndiaMFNavObtainer()

st.title("Indian Mutual funds dashboard")

# Get the name of the mutual fund from the user
st.subheader("Mutual fund details")
fund_name = st.text_input('Mutual fund name')

# Show the results sorted by score
funds_df = india_mf_nav_obtainer.fuzzy_search_mf_by_name(fund_name)
st.dataframe(funds_df)


# Get Id of the mutual fund to find
fund_id = st.text_input('Scheme code for the fund (from above table)')
fund_df = india_mf_nav_obtainer.get_historical_nav_for_mf(fund_id)

# Show the NAV values since inception
if fund_df is not None:
    fund_df_transformed = utils.transform_mutual_fund_df(fund_df)
    st.line_chart(fund_df_transformed['nav'])

    # Returns for 1, 3 and 5 years

    # Metrics - variance, SD, Min, max, average and median NAV values




