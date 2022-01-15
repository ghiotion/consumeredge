from flask import Flask
from flask_restful import Resource, Api
import sqlite3
from flask import g
import datetime
from collections import defaultdict


app = Flask(__name__)
api = Api(app)

DATABASE = 'finance.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

class Historical:
    def get_query(self, ticker, start_date, end_date):
        return f"select name, ticker, date, close, high, low\
            from historical inner \
            join companies on companies.company_id = historical.company_id \
            where companies.ticker='{ticker}' \
            and historical.date <= '{start_date}' \
            and historical.date >= '{end_date}' \
            order by historical.date;"

class GetHistorical(Resource, Historical):
    def get(self, ticker, start_date, end_date):      
        cur = get_db().cursor()
        cur.execute(self.get_query(ticker, start_date, end_date))
        rows = cur.fetchall()
        default_grouping = {}
        for item in rows:
            default_grouping.setdefault(item[2], [item[0], item[1], item[3], (item[4], item[5])])

        return default_grouping

class GetHistoricalGroup(Resource, Historical):
    def get(self, ticker, start_date, end_date):      
        cur = get_db().cursor()
        cur.execute(self.get_query(ticker, start_date, end_date))
        rows = cur.fetchall()
        weekly_grouping = defaultdict(list)

        for item in rows:
            t = datetime.datetime.strptime(item[2], '%Y-%m-%d')
            key = f'{t.isocalendar().year} - {t.isocalendar().week}'
            weekly_grouping[key].append([item[2], item[0], item[1], item[3], (item[4], item[5])])
        
        return weekly_grouping

class GetCompanies(Resource):
    def get(self):
        cur = get_db().cursor()
        cur.execute("select * from companies")
        rows = cur.fetchall()
        return rows        

api.add_resource(GetHistorical, '/api/v1/historical/<ticker>/<start_date>/<end_date>')
api.add_resource(GetHistoricalGroup, '/api/v1/historicalgroup/<ticker>/<start_date>/<end_date>')
api.add_resource(GetCompanies, '/api/v1/companies')

if __name__ == '__main__':
    app.run(debug=True)
