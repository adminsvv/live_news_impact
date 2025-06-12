#!/usr/bin/env python
# coding: utf-8

# In[15]:


from pymongo import MongoClient
import re

import pandas as pd
from openai import OpenAI
import yfinance as yf
import json

import yfinance as yf

import numpy as np
from datetime import datetime, timedelta,date,time as dt_time
from datetime import datetime, time
import os
pd.options.display.float_format = '{:.2f}'.format
import time
import time as time_module
from pandas.io.formats.style import Styler
import warnings
import streamlit as st

pd.set_option('display.max_columns', None)

# Set max column width to None (means unlimited)
pd.set_option('display.max_colwidth', None)

# If you want to see all rows too
pd.set_option('display.max_rows', None)

# Suppress all warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",  # 👈 enables wide mode
    page_title="Live News Impact"

)



CREDENTIALS = {
    "news_impact": "news_ib"

}



def login_block():
    """Returns True when user is authenticated."""
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"]:
        return True

    # ── Login form ──
    st.title("🔐 Login required")
    with st.form("login_form", clear_on_submit=False):
        user = st.text_input("Username")
        pwd  = st.text_input("Password", type="password")
        submit = st.form_submit_button("Log in")

    if submit:
        if user in CREDENTIALS and pwd == CREDENTIALS[user]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ Incorrect username or password")
    return False
if not login_block():
    st.stop()
# In[9]:
start_time = dt_time(9, 0)   # 09:00 AM
end_time = dt_time(15, 45) 
now = datetime.now().time()
refresh_data = st.checkbox("Refresh Data")  
st.title("Live News Dashboard - Stock with +-3%")
############# news from bse########
if refresh_data:
    MONGODB_URI = "mongodb+srv://prachi:Akash5555@stockgpt.fryqpbi.mongodb.net/"

    client_mongo = MongoClient(MONGODB_URI)
    db = client_mongo["CAG_CHATBOT"]
    collection_bse_news = db["ProcessedNews"]
    collection_live_squack=db["news_livesquack"]

    # Query the data
    # To get all docs, just use: docs = collection.find()

    today = datetime.today().date()

    # Yesterday's date
    yesterday = today - timedelta(days=1)

    dates = [today, yesterday]
    query = {
        "$or": [
            {"dt_tm": {"$regex": f"^{d}"}} for d in dates
        ]
    }
    docs = list(collection_bse_news.find(query))
    # Flatten nested 'symbolmap' for each document
    def flatten_doc(doc):
        symbolmap = doc.pop("symbolmap", {})
        # Add symbolmap keys to top level with prefix if needed
        for key, value in symbolmap.items():
            # If value is dict (e.g., symbolmap itself contains nested dict), flatten further if needed
            doc[key] = value
        # Convert ObjectId to string for DataFrame
        doc["_id"] = str(doc["_id"])
        return doc

    # Apply flattening
    flat_docs = [flatten_doc(doc) for doc in docs]

    # Create DataFrame
    df_bse = pd.DataFrame(flat_docs)
    df_bse.head()
    df_bse = df_bse.rename(columns={"NSE": "stock"})

    df_bse=df_bse.rename(columns={"pdf_link_live": "news link"})
    df_bse=df_bse.rename(columns={"shortsummary": "short summary"})
    df_bse=df_bse.rename(columns={"impactscore": "impact score"})

    df_bse=df_bse[['stock','news link','impact','impact score','sentiment','short summary','dt_tm']]
    df_bse.head(2)


    # In[13]:


    #########news from livesqack

    docs_livesquack = list(collection_live_squack.find(query))
    flat_docs_livesquack = [flatten_doc(doc) for doc in docs_livesquack]

    # Create DataFrame
    df_livesquack= pd.DataFrame(flat_docs_livesquack)
    df_livesquack.head()
    df_livesquack=df_livesquack.rename(columns={"nse_symbol": "stock"})
    df_livesquack['news link']=""
    df_livesquack=df_livesquack[['stock','news link','impact','impact score','sentiment','short summary','dt_tm']]
    df_livesquack.head(2)


    # In[14]:


    df_merged_all_news = pd.concat([df_livesquack, df_bse], ignore_index=True)

    df_merged_all_news.head()


    # In[17]:


    df_merged_all_news['stock'].nunique()


    # In[18]:


    # get yahoo data for the news stocks



    tickers = [f"{s}.NS" for s in df_merged_all_news['stock'].unique()]
    data = yf.download(
        tickers=tickers,
        period="2d",
        interval="1d",
        auto_adjust=True,
        progress=False
    )

    data= data.unstack(level=0)
    data = data.unstack(level=0)
    data = data.reset_index()
    data.head(2)


    # In[20]:


    data = data.sort_values(['Ticker', 'Date'])

    # Calculate pct_change for Close by ticker
    data['pct_change'] = data.groupby('Ticker')['Close'].pct_change() * 100

    # Keep only the last row per ticker (i.e., today's close and pct change from yesterday)
    latest = data.groupby('Ticker').tail(1).reset_index(drop=True)
    latest['stock'] = latest['Ticker'].str.replace('.NS', '')
    final_df = latest[['stock', 'Close', 'pct_change']]
    final_df=final_df[(final_df['pct_change'] < -3) | (final_df['pct_change'] > 3)]
    final_df.head(20)


    # In[21]:


    ############ merge news and stocks data

    final_df=final_df.merge(df_merged_all_news,how='left',on='stock')
    final_df.head()




    def row_color(pct, sentiment):
        if isinstance(sentiment, str):
            sentiment = sentiment.lower()
        if sentiment == 'positive':
            return 'positive'
        elif sentiment == 'negative':
            return 'negative'
        else:
            return 'neutral'

    rows = []
    for _, row in final_df.iterrows():
        pct_class = row_color(row['pct_change'], row['sentiment'])
        try:
            pct_value = float(row['pct_change'])
            pct_str = f"{pct_value:+.2f}%"
        except:
            pct_str = str(row['pct_change'])
        news_link = row.get('news link', '')
        if news_link and str(news_link).strip():
            news_link_html = f'<a href="{news_link}" target="_blank" class="news-link">PDF</a>'
        else:
            news_link_html = ''
        rows.append(f"""
        <tr>
            <td>{row['stock']}</td>
            <td>{news_link_html}</td>
            <td class="{pct_class}">{pct_str}</td>
            <td>{row['impact']}</td>
            <td><span class="impact-score">{row['impact score']}</span></td>
            <td class="{pct_class}">{row['sentiment']}</td>
            <td class="summary">{row['short summary']}</td>
            <td>{row['dt_tm']}</td>
        </tr>
        """)

    html_table = '\n'.join(rows)


    full_html_code = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Stock News Impact Dashboard</title>
        <style>
            thead th {{
                position: sticky;
                top: 0;
                background: #e7eef7;
                z-index: 2;
            }}
            body {{
                font-family: 'Segoe UI', Arial, sans-serif;
                background: #f7f8fa;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 1200px;
                margin: 40px auto;
                background: #fff;
                border-radius: 16px;
                box-shadow: 0 2px 8px rgba(60,60,60,0.11);
                padding: 30px;
            }}
            h2 {{
                margin-bottom: 24px;
                color: #26313e;
                letter-spacing: 1px;
            }}
            .table-scroll {{
                overflow-x: auto;
                max-height: 600px;
                border-radius: 10px;
            }}
            table {{
                border-collapse: collapse;
                min-width: 1100px;
                width: 100%;
                background: #fff;
            }}
            th, td {{
                text-align: left;
                padding: 12px 14px;
                border-bottom: 1px solid #ececec;
                vertical-align: top;
            }}
            th {{
                background: #e7eef7;
                font-weight: 600;
                cursor: pointer;
                user-select: none;
            }}
            tr:hover {{
                background: #f5faff;
            }}
            .positive {{
                color: #15af4c;
                font-weight: bold;
            }}
            .negative {{
                color: #e03b3b;
                font-weight: bold;
            }}
            .neutral {{
                color: #666;
                font-weight: bold;
            }}
            .impact-score {{
                background: #f3f3f3;
                padding: 4px 10px;
                border-radius: 8px;
                font-size: 1em;
                display: inline-block;
            }}
            .summary {{
                max-width: 420px;
                overflow-x: auto;
                white-space: pre-line;
                font-size: 1em;
                color: #384357;
            }}
            .news-link {{
                color: #0069c2;
                text-decoration: none;
                word-break: break-all;
            }}
            .news-link:hover {{
                text-decoration: underline;
            }}
            th.sort-asc::after {{
                content: " ▲";
                font-size: 1em;
                color: #26313e;
            }}
            th.sort-desc::after {{
                content: " ▼";
                font-size: 1em;
                color: #26313e;
            }}
            @media (max-width: 600px) {{
                .container {{
                    padding: 7px;
                }}
                .summary {{
                    max-width: 170px;
                }}
            }}
        </style>
    </head>
    <body>
    <div class="container">
        <h2>Stock News Impact Dashboard</h2>
        <div class="table-scroll">
            <table id="impact-table">
                <thead>
                    <tr>
                        <th>Stock</th>
                        <th>News Link</th>
                        <th>% Change</th>
                        <th>Impact</th>
                        <th>Impact Score</th>
                        <th>Sentiment</th>
                        <th>Summary</th>
                        <th>Date/Time</th>
                    </tr>
                </thead>
                <tbody>
                    {html_table}
                </tbody>
            </table>
        </div>
    </div>
    <script>
    document.addEventListener('DOMContentLoaded', function() {{
        const table = document.getElementById('impact-table');
        let lastSortedCol = null;
        let lastSortAsc = true;

        function getCellValue(tr, idx) {{
            const cell = tr.children[idx];
            if (cell.querySelector('a')) {{
                return cell.querySelector('a').textContent.trim();
            }}
            return cell.textContent.trim();
        }}

        function comparer(idx, asc, type) {{
            return function(a, b) {{
                let v1 = getCellValue(asc ? a : b, idx);
                let v2 = getCellValue(asc ? b : a, idx);
                if (type === 'number') {{
                    v1 = parseFloat(v1.replace(/[^\d\.\-]+/g, '')) || 0;
                    v2 = parseFloat(v2.replace(/[^\d\.\-]+/g, '')) || 0;
                }} else if (type === 'date') {{
                    v1 = Date.parse(v1) || 0;
                    v2 = Date.parse(v2) || 0;
                }} else {{
                    v1 = v1.toLowerCase();
                    v2 = v2.toLowerCase();
                }}
                return v1 > v2 ? 1 : v1 < v2 ? -1 : 0;
            }}
        }}

        Array.from(table.querySelectorAll('th')).forEach(function(th, idx) {{
            th.addEventListener('click', function() {{
                // Remove all sort indicators
                table.querySelectorAll('th').forEach(header => {{
                    header.classList.remove('sort-asc', 'sort-desc');
                }});

                // Decide the new direction
                let asc = true;
                if (lastSortedCol === idx) {{
                    asc = !lastSortAsc;
                }}
                lastSortedCol = idx;
                lastSortAsc = asc;
                th.classList.add(asc ? 'sort-asc' : 'sort-desc');

                // Choose sorting type
                let type = 'string';
                if (idx === 2 || idx === 4) type = 'number'; // % Change, Impact Score
                if (idx === 7) type = 'date'; // Date/Time

                const tbody = table.tBodies[0];
                // Sort and reattach rows
                Array.from(tbody.querySelectorAll('tr'))
                    .sort(comparer(idx, asc, type))
                    .forEach(tr => tbody.appendChild(tr));
            }});
        }});
    }});
    </script>

    </body>
    </html>
    """
     st.markdown(full_html_code, unsafe_allow_html=True)
    #st.components.v1.html(full_html_code, height=800, scrolling=True)
    #time_module.sleep(60)


# with open('D:/Chat Gpt/stock_news_impact_all_june12.html', 'w', encoding='utf-8') as f:
#     f.write(full_html_code)

# print("HTML dashboard saved as 'stock_news_dashboard.html'. Open it in your browser!")

# ---------- END PYTHON CODE ----------

