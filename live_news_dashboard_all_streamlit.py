import asyncio
import platform
import re
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date, time as dt_time
from pymongo import MongoClient
import streamlit as st
import streamlit.components.v1 as components
import warnings

class StockNewsDashboard:
    def __init__(self):
        # Pandas settings
        pd.options.display.float_format = '{:.2f}'.format
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        warnings.filterwarnings("ignore")

        # Streamlit page config
        st.set_page_config(
            layout="wide",
            page_title="Live News Impact"
        )

        # Constants
        self.CREDENTIALS = {
            "news_impact": "news_ib"
        }
        self.MONGODB_URI = st.secrets["mongodb"]["uri"]
        self.start_time = dt_time(9, 0)  # 09:00 AM
        self.end_time = dt_time(15, 45)

    def login_block(self):
        """Returns True when user is authenticated."""
        if "authenticated" not in st.session_state:
            st.session_state["authenticated"] = False

        if st.session_state["authenticated"]:
            return True

        st.title("🔐 Login required")
        with st.form("login_form", clear_on_submit=False):
            user = st.text_input("Username")
            pwd = st.text_input("Password", type="password")
            submit = st.form_submit_button("Log in")

        if submit:
            if user in self.CREDENTIALS and pwd == self.CREDENTIALS[user]:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("❌ Incorrect username or password")
        return False

    def fetch_bse_news(self):
        client_mongo = MongoClient(self.MONGODB_URI)
        db = client_mongo["CAG_CHATBOT"]
        collection_bse_news = db["ProcessedNews"]
        collection_live_squack = db["news_livesquack"]

        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        dates = [today, yesterday]
        query = {
            "$or": [
                {"dt_tm": {"$regex": f"^{d}"}} for d in dates
            ]
        }
        docs = list(collection_bse_news.find(query))

        def flatten_doc(doc):
            symbolmap = doc.pop("symbolmap", {})
            for key, value in symbolmap.items():
                doc[key] = value
            doc["_id"] = str(doc["_id"])
            return doc

        if docs:
            flat_docs = [flatten_doc(doc) for doc in docs]
            df_bse = pd.DataFrame(flat_docs)
            if not df_bse.empty:
                df_bse = df_bse.rename(columns={
                    "NSE": "stock",
                    "pdf_link_live": "news link",
                    "shortsummary": "short summary",
                    "impactscore": "impact score"
                })
                expected_cols = ['stock', 'news link', 'impact', 'impact score', 'sentiment', 'short summary', 'dt_tm']
                df_bse = df_bse[[col for col in expected_cols if col in df_bse.columns]]
                return df_bse
        expected_cols = ['stock', 'news link', 'impact', 'impact score', 'sentiment', 'short summary', 'dt_tm']
        return pd.DataFrame(columns=expected_cols)

    def fetch_livesquack_news(self):
        client_mongo = MongoClient(self.MONGODB_URI)
        db = client_mongo["CAG_CHATBOT"]
        collection_live_squack = db["news_livesquack"]

        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        dates = [today, yesterday]
        query = {
            "$or": [
                {"dt_tm": {"$regex": f"^{d}"}} for d in dates
            ]
        }
        docs_livesquack = list(collection_live_squack.find(query))

        def flatten_doc(doc):
            symbolmap = doc.pop("symbolmap", {})
            for key, value in symbolmap.items():
                doc[key] = value
            doc["_id"] = str(doc["_id"])
            return doc

        flat_docs_livesquack = [flatten_doc(doc) for doc in docs_livesquack]
        df_livesquack = pd.DataFrame(flat_docs_livesquack)
        df_livesquack = df_livesquack.rename(columns={"nse_symbol": "stock"})
        df_livesquack['news link'] = ""
        df_livesquack = df_livesquack[['stock', 'news link', 'impact', 'impact score', 'sentiment', 'short summary', 'dt_tm']]
        return df_livesquack

    def fetch_yahoo_data(self, tickers):
        data = yf.download(
            tickers=[f"{s}.NS" for s in tickers[:50]],
            period="2d",
            interval="1d",
            auto_adjust=True,
            progress=False
        )
        data = data.unstack(level=0).unstack(level=0).reset_index()
        data = data.sort_values(['Ticker', 'Date'])
        data['pct_change'] = data.groupby('Ticker')['Close'].pct_change() * 100
        latest = data.groupby('Ticker').tail(1).reset_index(drop=True)
        latest['stock'] = latest['Ticker'].str.replace('.NS', '')
        final_df = latest[['stock', 'Close', 'pct_change']]
        final_df = final_df[(final_df['pct_change'] < -3) | (final_df['pct_change'] > 3)]
        return final_df

    def row_color(self, pct, sentiment):
        if isinstance(sentiment, str):
            sentiment = sentiment.lower()
        if sentiment == 'positive':
            return 'positive'
        elif sentiment == 'negative':
            return 'negative'
        else:
            return 'neutral'

    def generate_html_table(self, final_df):
        rows = []
        for _, row in final_df.iterrows():
            pct_class = self.row_color(row['pct_change'], row['sentiment'])
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
        return '\n'.join(rows)

    def generate_html(self, html_table):
        return f"""
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
                .search-bar {{
                    margin-bottom: 20px;
                    display: flex;
                    justify-content: flex-start;
                    gap: 10px;
                }}
                .search-bar input {{
                    padding: 8px 12px;
                    width: 200px;
                    border: 1px solid #ccc;
                    border-radius: 8px;
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
            <div class="search-bar">
                <input type="text" id="stock-search" placeholder="Search stock...">
            </div>
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
            const searchInput = document.getElementById('stock-search');
            searchInput.addEventListener('keyup', function() {{
                const filter = this.value.toLowerCase();
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(row => {{
                    const stockCell = row.children[0];
                    const stockText = stockCell.textContent.toLowerCase();
                    row.style.display = stockText.includes(filter) ? '' : 'none';
                }});
            }});
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
                    table.querySelectorAll('th').forEach(header => {{
                        header.classList.remove('sort-asc', 'sort-desc');
                    }});
                    let asc = true;
                    if (lastSortedCol === idx) {{
                        asc = !lastSortAsc;
                    }}
                    lastSortedCol = idx;
                    lastSortAsc = asc;
                    th.classList.add(asc ? 'sort-asc' : 'sort-desc');
                    let type = 'string';
                    if (idx === 2 || idx === 4) type = 'number';
                    if (idx === 7) type = 'date';
                    const tbody = table.tBodies[0];
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

    async def run(self):
        if not self.login_block():
            st.stop()

        st.title("Live News Dashboard - Stocks with +-3%")
        df_bse = self.fetch_bse_news()
        df_livesquack = self.fetch_livesquack_news()
        df_merged_all_news = pd.concat([df_livesquack, df_bse], ignore_index=True)
        final_df = self.fetch_yahoo_data(df_merged_all_news['stock'].unique())
        final_df = final_df.merge(df_merged_all_news, how='left', on='stock')
        final_df['dt_tm'] = pd.to_datetime(final_df['dt_tm'], errors='coerce')

        distinct_stock_count = final_df['stock'].nunique()
        time_threshold = datetime.now() - timedelta(minutes=15) + timedelta(hours=5, minutes=30)
        recent_df = final_df[final_df['dt_tm'] >= time_threshold]
        recent_stock_count = recent_df['stock'].nunique()

        col1, col2 = st.columns(2)
        with col1:
            st.metric(label="📈 Total Distinct Stocks", value=distinct_stock_count)
        with col2:
            st.metric(label="⏱️ Stocks Active in Last 15 Min", value=recent_stock_count)

        html_table = self.generate_html_table(final_df)
        full_html_code = self.generate_html(html_table)
        st.components.v1.html(full_html_code, height=800, scrolling=True)

    def main(self):
        if platform.system() == "Emscripten":
            asyncio.ensure_future(self.run())
        else:
            asyncio.run(self.run())

if __name__ == "__main__":
    dashboard = StockNewsDashboard()
    dashboard.main()
