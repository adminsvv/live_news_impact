import asyncio
import platform
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
import streamlit as st
from pandas.tseries.offsets import BDay
import streamlit.components.v1 as components

st.write(datetime.utc.now())

class StockNewsDashboard:
    def __init__(self):
        st.set_page_config(layout="wide",page_title="Live News Impact")
        self.CREDENTIALS = {"news_impact": "news_ib"}
        self.MONGODB_URI = st.secrets["mongodb"]["uri"]
        self.db = MongoClient(self.MONGODB_URI)["CAG_CHATBOT"]
        self.collection = self.db['NewsImpactDashboard']
        self.df = pd.DataFrame()

    def login_block(self):
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

    def filter_data(self):
        df = pd.DataFrame()
        full_day = st.checkbox("Include full day news")
        last_working_day = (datetime.now() - BDay(1)).replace(
            hour=0 if full_day else 15,
            minute=0 if full_day else 30,
            second=1 if full_day else 0,
            microsecond=0
        )
        
        df = pd.DataFrame(list(self.collection.find({
            "dt_tm": {
                "$gte": last_working_day,
                "$lte": datetime.now()
            },
            
            "duplicate": False,
            "stock": { "$ne": None },
            "$nor": [
                {
                    "$and": [
                        { "impact score": { "$ne": None, "$lte": 4 } },
                        { "sentiment": { "$ne": None, "$eq": "Neutral" } }
                    ]
                }
            ],
            "$or": [
                { "pct_change": { "$lte": -3 } },
                { "pct_change": { "$gte": 3 } },
                { "pct_change": "Post Market News" }
            ],
            "short summary": { "$ne": None }
        })))
        if df.empty:
            return df
        st.write(df)
        df['highlight'] = False
        df.loc[(((df['sentiment'] == 'Positive') & (pd.to_numeric(df['pct_change'], errors='coerce') <= -3)) | ((df['sentiment'] == 'Negative') & (pd.to_numeric(df['pct_change'], errors='coerce') >= 3))), ['sentiment', 'impact score', 'highlight']] = ['Neutral', 4, True]
        return df

    def row_color(self, highlight, sentiment):
        if highlight:
            return 'overwrite'
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
            pct_class = self.row_color(row['highlight'], row['sentiment'])
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

            tr_class = ' class="overwrite"' if pct_class == 'overwrite' else ''
            other_class = f' class="{pct_class}"'if tr_class == '' else ''

            rows.append(f"""
            <tr {tr_class}>
                <td>{row['stock']}</td>
                <td>{news_link_html}</td>
                <td {other_class}>{pct_str}</td>
                <td>{row['impact']}</td>
                <td><span class="impact-score">{row['impact score']}</span></td>
                <td {other_class}>{row['sentiment']}</td>
                <td class="summary">{row['short summary']}</td>
                <td>{row['dt_tm']}</td>
            </tr>
            """)
        return '\n'.join(rows)

    def generate_html(self, html_table):
        return  f"""
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
                    # tr:hover {{
                    #     background: #f5faff;
                    # }}
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
                    .overwrite {{
                        color: #666;
                        font-weight: bold;
                        background-color: #b3d9ff;
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

                // Live stock name search
                searchInput.addEventListener('keyup', function() {{
                    const filter = this.value.toLowerCase();
                    const rows = table.querySelectorAll('tbody tr');

                    rows.forEach(row => {{
                        const stockCell = row.children[0];
                        const stockText = stockCell.textContent.toLowerCase();
                        row.style.display = stockText.includes(filter) ? '' : 'none';
                    }});
                }});

                // Sorting
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
        
        if 'refresh_data' not in st.session_state:
            st.session_state['refresh_data'] = False

        if st.button("Refresh Data"):
            st.session_state['refresh_data'] = True
            self.df = self.filter_data()
            st.rerun()

        if self.df.empty:
            self.df = self.filter_data()
            if self.df.empty:
                st.warning('No data Found')
                st.stop()
        
        self.df['dt_tm'] = pd.to_datetime(self.df['dt_tm'], errors='coerce')
        distinct_stock_count = self.df['stock'].nunique()
        time_threshold = datetime.now() - timedelta(minutes=15) + timedelta(hours=5, minutes=30)
        recent_df = self.df[self.df['dt_tm'] >= time_threshold]
        recent_stock_count = recent_df['stock'].nunique()

        col1, col2 = st.columns(2)
        with col1:
            st.metric(label="📈 Total Distinct Stocks", value=distinct_stock_count)
        with col2:
            st.metric(label="⏱️ Stocks Active in Last 15 Min", value=recent_stock_count)

        html_table = self.generate_html_table(self.df)
        full_html_code = self.generate_html(html_table)
        st.components.v1.html(full_html_code, height=800, scrolling=True)
        st.session_state['refresh_data'] = False

    def main(self):

        if platform.system() == "Emscripten":
            asyncio.ensure_future(self.run())
        else:
            asyncio.run(self.run())

if __name__ == "__main__":
    dashboard = StockNewsDashboard()
    dashboard.main()
