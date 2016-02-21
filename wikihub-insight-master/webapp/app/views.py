from flask import render_template, jsonify
from flask import request, Response
from app import app
from helpers import getYearlyData
from helpers import getData
from helpers import getRangedData
from helpers import getPoint
from helpers import convertDateKeys
import json
import happybase

# db = mdb.connect(user="api", host="127.0.0.1", passwd="msqlapi", db="gdelt", charset='utf8')
# c = Connection(host='127.0.0.1', port=7070)
# t1 = c.table('wikiedits')
# t2 = c.table('wikieditssmall')


# ROUTING/VIEW FUNCTIONS
@app.route('/')
@app.route('/index')
def index():
    # Renders index.html.
    return render_template('index.html')

@app.route('/author')
def contact():
    # Renders author.html.
    return render_template('author.html')

@app.route('/page/', methods=['GET'])
def page(): 
    qs = request.query_string
    if request.args.get("title") is None:
        raise InvalidAPIUsage('You need to pass a title parameter', status_code=400)
    titleparam = request.args.get("title")
    caldaily_data = getRangedData(titleparam, time_granularity = "D", start="20140401", end="20140930")
    caldaily_data_js = convertDateKeys(caldaily_data, JS=False)
    yearly_data = getData(titleparam, time_granularity = "Y")
    monthly_data = getData(titleparam, time_granularity = "M")
    daily_data = getData(titleparam, time_granularity = "D")
    yearly_data_js = convertDateKeys(yearly_data)
    monthly_data_js = convertDateKeys(monthly_data)
    daily_data_js = convertDateKeys(daily_data)
    ydata = []
    mdata = []
    ddata = []
    for t1 in sorted(yearly_data_js.items(), key=lambda x: x[0]):
        ydata.append(list(t1))
    for t2 in sorted(monthly_data_js.items(), key=lambda x: x[0]):
        mdata.append(list(t2))
    for t3 in sorted(daily_data_js.items(), key=lambda x: x[0]):
        ddata.append(list(t3))
    return render_template('page.html', title=titleparam, cal_data=caldaily_data_js, ydata=ydata, mdata=mdata, ddata=ddata)

@app.route('/demo', methods=['GET'])
def demo():
    # Renders demo.html.
    return render_template('demo.html')

@app.route('/test/', methods=['GET'])
def test():
    return request.query_string

@app.route('/testquery/', methods=['GET'])
def testquery():
    rs = request.query_string
    if request.args.get("query") is not None:
        hqs = request.args.get("query")
        return getPoint(hqs)
    else:
        return None

@app.route('/testranged/', methods=['GET'])
def testranged():
    rs = request.query_string
    if request.args.get("query") is not None:
        hqs = request.args.get("query")
        return getPoint(hqs)
    else:
        return None

@app.route('/testcalendar/', methods=['GET'])
def test_calendar(): 
    qs = request.query_string
    if request.args.get("title") is None:
        raise InvalidAPIUsage('You need to pass a title parameter', status_code=400)
    titleparam = request.args.get("title")
    caldaily_data = getRangedData(titleparam, time_granularity = "D", start="2014-04-01", end="2014-09-30")
    cal_data = convertDateKeys(caldaily_data, JS=False)
    return render_template('calendar.html', title=titleparam, cal_data=cal_data)

@app.route('/testhighcharts/', methods=['GET'])
def test_charts(): 
    qs = request.query_string
    if request.args.get("title") is None:
        raise InvalidAPIUsage('You need to pass a title parameter', status_code=400)
    titleparam = request.args.get("title")
    yearly_data = getData(titleparam, time_granularity = "Y")
    monthly_data = getData(titleparam, time_granularity = "M")
    yearly_data_js = convertDateKeys(yearly_data)
    monthly_data_js = convertDateKeys(monthly_data)
    mdata=[]
    ydata=[]
    for t1 in sorted(yearly_data_js.items(), key=lambda x: x[0]):
        ydata.append(list(t1))
    for t2 in sorted(monthly_data_js.items(), key=lambda x: x[0]):
        mdata.append(list(t2))
    return render_template('highcharts.html', title=titleparam, ydata=ydata, mdata=mdata)

@app.route('/api', methods=['GET'])
def query_api():
    rs = request.query_string
    if request.args.get("title") is None:
        raise InvalidAPIUsage('You need to pass a title parameter', status_code=400)
    else:
        title=request.args.get("title")
    if request.args.get("granularity") is None:
        raise InvalidAPIUsage('You need to set a granularity among (Yearly,Monthly,Daily)', status_code=400)
    else:
        granularity = request.args.get("granularity")
        if granularity.lower() not in ("yearly","monthly","daily"):
            raise InvalidAPIUsage('Granularity is one of (Yearly,Monthly,Daily)', status_code=400)
        else:
            gr_raw=request.args.get("granularity")
            gr=gr_raw[0].upper()
    if request.args.get("start") is None or request.args.get("end") is None:
        raise InvalidAPIUsage('You need to set both start and end parameters', status_code=400)
    else:
        start=request.args.get("start")
        end=request.args.get("end")
        data = getRangedData(title,time_granularity=gr,start=start,end=end)
        return jsonify(data)
    #data = getYearlyData(title)
    #data = getData(title, time_granularity=g_code)
    # Queries the DB and returns data
    # cur = db.cursor()   
    return "Error"   

@app.route('/api/<string:granularity>/<string:title>', methods=['GET'])
def rest_api(title,granularity):
    title_Hbase = title.replace('_',' ')
    if granularity.lower() not in ("yearly","monthly","daily"):
        raise InvalidAPIUsage('Granularity is one of (Yearly,Monthly,Daily)', status_code=400)
    g_code = granularity[0].upper()
    #qs = request.query_string
    #titleparam = request.args.get("title")
    #data = getYearlyData(title)
    #default_ranges = {"Y":("2001","2014"),"M":("2001-01","2014-09"),"D":("2001-01-15","2014-09-15")}
    data = getData(title, time_granularity=g_code)
    # Queries the DB and returns data
    # cur = db.cursor()   
    return jsonify(data)

# @app.route('/api/top', methods=['GET'])
# def api():
#     # Queries the DB and returns data
#     # cur = db.cursor()   
#     return render_template('api.html')