from main import consolidated
import streamlit as st

df = consolidated()
st.dataframe(df)

# to run this file you have to use below command
# streamlit run UI.py

