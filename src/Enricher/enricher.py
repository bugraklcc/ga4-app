import pandas as pd

class Enricher:
    @staticmethod
    def month_of_year_start_end_date_generate(year_month):
        start_date = pd.to_datetime(f"{year_month}01", format='%Y%m%d')
        end_date = f"1 - {start_date.days_in_month} {start_date.strftime('%B %Y')}"
        return end_date

    @staticmethod
    def calculate_CR_for_GUA(df, transactions_column_name, sessions_column_name):
        df[transactions_column_name] = df[transactions_column_name].fillna(0.0).astype(float)
        df[sessions_column_name] = df[sessions_column_name].fillna(0.0).astype(float)

        df['CR'] = (df[transactions_column_name] / df[sessions_column_name]) * 100

        df['CR'] = df['CR'].apply(lambda x: '%{0:.2f}'.format(x))
        return df

    @staticmethod
    def enrich_data(df):
        df = Enricher.calculate_CR_for_GUA(df, 'transactions', 'sessions')

        df['screenPageViews'] = df['screenPageViews'].astype(float)
        df['totalUsers'] = df['totalUsers'].astype(float)
        df['No of pages per user'] = df['screenPageViews'] / df['totalUsers']
        df['screenPageViews'] = df['screenPageViews'].astype(int)
        df['totalUsers'] = df['totalUsers'].astype(int)

        df['Date Range'] = df.apply(lambda x: Enricher.month_of_year_start_end_date_generate(
            year_month=x['yearMonth']), axis=1)

        df = df.rename(columns={
            "yearMonth": "Month Of Year",
            "averageSessionDuration": "Average Session Duration",
            "bounceRate": "Bounce Rate",
            "engagementRate": "Engagement Rate",
            "newUsers": "New Users",
            "screenPageViews": "Pageview",
            "sessions": "Sessions",
            "screenPageViewsPerSession": "No of pages per session",
            "sessionsPerUser": "No of session per user",
            "totalRevenue": "Revenue",
            "totalUsers": "Users",
            "transactions": "Transaction",
            "CR": "CR %"
        })

        return df[
            [
                "Month Of Year", "Date Range", "Users", "New Users", "Sessions", "Pageview", "No of pages per session", "No of session per user", "No of pages per user", "Average Session Duration", "Bounce Rate", "Engagement Rate", "CR %", "Transaction", "Revenue"
            ]
        ]
