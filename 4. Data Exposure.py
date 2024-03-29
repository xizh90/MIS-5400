import pyodbc
from flask import Flask, g, render_template, abort, request
import json

# Setup Flask
app = Flask(__name__)
app.config.from_object(__name__)

# Before / Teardown
@app.before_request
def before_request():
    try:
        g.sql_conn = pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL server};'      
                                    r'SERVER=DESKTOP-736C5K5;'  
                                    r'DATABASE=MIS 5400;'    
                                    r'Trusted_Connection=yes', autocommit=True)
    except Exception:
        abort(500, "No database connection could be established.")


@app.teardown_request
def teardown_request(exception):
    try:
        g.sql_conn.close()
    except AttributeError:
        pass

@app.route('/')
@app.route('/home', methods=['GET'])
def home():
    curs = g.sql_conn.cursor()
    query = 'select * from dbo.Zillow '
    curs.execute(query)

    columns = [column[0] for column in curs.description]
    data = []

    for row in curs.fetchall():
        data.append(dict(zip(columns, row)))
    return json.dumps(data, indent=4, sort_keys=True, default=str)

if __name__ == '__main__':
    app.run()
