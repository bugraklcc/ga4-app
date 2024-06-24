class Queries:
    @staticmethod
    def get_query_3(start_date, end_date):
        return {
            "dimensions": [
                {"name": "date"},
                {"name": "sessionSourceMedium"}
            ],
            "metrics": [{"name": "transactions"}],
            "dateRanges": [{"startDate": "2024-05-01", "endDate": "2024-05-30"}],
        }
