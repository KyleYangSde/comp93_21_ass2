import sqlite3
from flask import Flask
import requests
import urllib.request as req
from flask_restplus import Resource, Api
from flask_restplus import reqparse
import json
import time
import os


app = Flask(__name__)
api = Api(app, title='Data Service for World Bank Economic Indicators',
          description='COMP9321')
parser = reqparse.RequestParser()
parser.add_argument('indicator_id', type=str,
                    help='Rate cannot be converted', location="args")
parser.add_argument('query', type=str,
                    help='Rate cannot be converted', location="args")
parser.add_argument('orderby', type=str,
                    help='Rate cannot be converted', location="args")


def fetchID():
    conn = sqlite3.connect('z5177443.db')
    cursor = conn.cursor()
    cursor.execute("select max(id) from countries")
    conn.commit()
    result = cursor.fetchall()
    return result[0][0]


def sendReq(query):
    url = f'http://api.worldbank.org/v2/countries/all/indicators/{query}?date=2012:2017&format=json&per_page=1000'
    content = req.Request(url)
    data = req.urlopen(content).read()
    data = json.loads(data)
    if len(data) == 1:
        return 0
    return data


def sendReqPage2(query):
    url = f'http://api.worldbank.org/v2/countries/all/indicators/{query}?date=2012:2017&format=json&per_page=1000&page=2'
    content = req.Request(url)
    data = req.urlopen(content).read()
    data = json.loads(data)
    if len(data) == 1:
        return 0
    return data


def fetchCurrentTime():
    timestamp = int(time.time())
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    return dt


def getQ1(res):
    uri = f"/collections/{int(res[12])}"
    id = f"{int(res[12])}"
    creation_time = f"{res[11]}"
    indicator_id = f"{res[1]}"

    return {
        "uri": uri,
        "id": id,
        "creation_time": creation_time,
        "indicator_id": indicator_id
    }


def handlePost(data, query):
    # createDb()
    res = handleCommand(
        f"Select * from countries where indicator_id = '{query}'")
    if res:
        return getQ1(res[0]), 200
    else:
        id = fetchID()
        if id == None:
            id = 1
        else:
            id += 1
        # print("id:", id)
        updateTableId(data, id)
        res = handleCommand(
            f"Select * from countries where indicator_id = '{query}'")
        Q1 = getQ1(res[0])
        return Q1


def handleCommand(command):
    conn = sqlite3.connect('z5177443.db')
    cursor = conn.cursor()
    # print(command)
    # Insert a row of data
    cursor.execute(command)
    result = cursor.fetchall()
    conn.commit()
    conn.close()
    return result


@api.route('/collections')
@api.response(201, 'Created')
@api.response(404, 'Not Founded')
class Collections(Resource):
    @api.doc(params={"indicator_id": ""})
    def post(self):
        query = parser.parse_args()["indicator_id"]
        if not query:
            return {"message": "can not find query"}, 404
        data = sendReq(query)
        dataPage2 = sendReqPage2(query)

        if dataPage2[1]:
            data[1] = data[1] + dataPage2[1]

        if not data:
            return {"message": "invaild query"}, 404
        else:
            return handlePost(data, query), 201

    @api.doc(params={"orderby": ""})
    def get(self):
        query = parser.parse_args()["orderby"]
        if not query:
            return {"message": "can not find query"}, 404
        res = handleOrderBy(query)
        if res[0] == {"message": "No data in the database!"}:
            return {"message": "No data in the database!"}, 404
        else:
            # print(res)
            splitQuery = query.split(",")
            # print(splitQuery)
            # will the id place in order?????????
            if len(splitQuery) == 3:
                res = sortId(res, splitQuery)
            elif len(splitQuery) == 2:
                for i in splitQuery:
                    if i[1:] == "creation_time":
                        res = sortCreation(res, splitQuery)
                    if i[1:] == "id":
                        res = sortId(res, splitQuery)
            elif len(splitQuery) == 1:
                if splitQuery[0][1:] == "id":
                    res = sortId(res, splitQuery)
                if splitQuery[0][1:] == "creation_time":
                    res = sortCreation(res, splitQuery)
                if splitQuery[0][1:] == "indicator":
                    res = sortIndicator(res, splitQuery)
            else:
                return {"message": "bad query"}, 404
            return res, 200

# order ["+id"]


def sortId(res, order):
    a = []
    new = []

    for j in res:
        a.append(j["id"])
    a = sorted(a)
    for i in order:
        if i[1:] == "id":
            sign = i[0]
    # a 2,3 // b
    if sign == "+":
        for i in a:
            for j in res:
                if j["id"] == i:
                    new.append(j)
    if sign == "-":
        for i in a[::-1]:
            for j in res:
                if j["id"] == i:
                    new.append(j)
    # print("shit", new)
    return new


def sortCreation(res, order):
    a = []
    new = []

    for j in res:
        timeArray = time.strptime(j["creation_time"], "%Y-%m-%d %H:%M:%S")
        a.append(time.mktime(timeArray))
    a = sorted(a)
    for i in order:
        if i[1:] == "creation_time":
            sign = i[0]
    # a 2,3 // b
    if sign == "+":
        for i in a:
            for j in res:
                timeArray = time.localtime(i)
                otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                if j["creation_time"] == otherStyleTime:
                    new.append(j)

    if sign == "-":
        for i in a[::-1]:
            for j in res:
                timeArray = time.localtime(i)
                otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                if j["creation_time"] == otherStyleTime:
                    new.append(j)
    # print("shit", new)
    return new


def sortIndicator(res, order):
    a = []
    new = []
    for j in res:
        a.append(j["indicator"])
    a = sorted(a)
    for i in order:
        if i[1:] == "indicator":
            sign = i[0]

    # a 2,3 // b
    if sign == "+":
        for i in a:
            for j in res:
                if j["indicator"] == i:
                    new.append(j)
    if sign == "-":
        for i in a[::-1]:
            for j in res:
                if j["indicator"] == i:
                    new.append(j)
    # print("shit", new)
    return new


def handleOrderBy(query):
    sql = "select * from countries"
    res = handleCommand(sql)
    if res:
        return getQ3(res)
    else:
        return {"message": "No data in the database!"}, 404


def getQ3(res):
    entries = []
    sameId = []
    for i in res:
        if i[12] not in sameId:
            singleRecord = {}
            singleRecord["uri"] = f"/collections/{i[12]}"
            singleRecord["id"] = i[12]
            singleRecord["creation_time"] = i[11]
            singleRecord["indicator"] = i[1]
            sameId.append(i[12])
            entries.append(singleRecord)
    return entries
####################################################################
@api.route('/collections/<int:id>')
@api.response(200, 'OK')
@api.response(404, 'Not Founded')
class delete(Resource):
    def delete(self, id):
        return handleDelete(id)

    def get(self, id):
        return handleGet(id)


def getQ4(res, id):
    # uri = f"/collections/{int(res[0])}"
    id = f"{id}"
    creation_time = f"{res[0][11]}"
    indicator_id = f"{res[0][1]}"
    indicator_value = f"{res[0][2]}"
    entries = []

    for i in res:
        singleRecord = {}
        singleRecord["country"] = i[4]
        singleRecord["date"] = i[6]
        singleRecord["value"] = i[7]
        entries.append(singleRecord)

    return {
        "id": id,
        "indicator": indicator_id,
        "indicator_value": indicator_value,
        "creation_time": creation_time,
        "entries": entries
    }


def handleGet(id):
    query = f"Select * from countries where id = {id}"
    res = handleCommand(query)
    if res:

        return getQ4(res, id), 200
    else:
        return {"message": f"The collection {id} cannot be founded from the database!"}, 404


def handleDelete(id):
    # print(id)
    # print(type(id))
    query = f"Select * from countries where id = {id}"
    res = handleCommand(query)

    if res:
        query = f"Delete from countries where id = {id}"
        handleCommand(query)
        res = handleCommand(f"Select * from countries where id = {id}")
        # print(res)
        return {"message": f"The collection {id} was removed from the database!", "id": f"{id}"}, 200
    else:
        return {"message": f"The collection {id} cannot be founded from the database!"}, 404

####################################################################
@api.route('/collections/<int:id>/<int:year>/<string:country>')
@api.response(200, 'OK')
@api.response(404, 'Not Founded')
class get(Resource):
    def get(self, id, year, country):
        return handleGetIdYear(id, year, country)


def handleGetIdYear(id, year, country):
    query = f'Select * from countries where id={id} and date="{year}" and country_value="{country}"'
    res = handleCommand(query)
    if res:
        return getQ5(res[0])
    else:
        return {"message": f"The collection {id},{year},{country} cannot be founded from the database!"}, 404


def getQ5(res):
    id = f"{res[12]}"
    indicator_id = f"{res[1]}"
    country = f"{res[4]}"
    year = f"{res[6]}"
    value = f"{res[7]}"

    return {
        "id": id,
        "indicator": indicator_id,
        "country": country,
        "year": year,
        "value": value
    }


@api.route('/collections/<int:id>/<int:year>')
@api.response(200, 'OK')
@api.response(404, 'Not Founded')
@api.response(400, 'Bad Request')
class q6(Resource):
    @api.doc(params={"query": ""})
    def get(self, id, year):
        query = parser.parse_args()["query"]
        if not query:

            return handleGetLimit(id, year, [1]), 200
        if "+" == query[0] or "-" == query[0] and query[1:].isdigit():
            return handleGetLimit(id, year, query), 200
        else:
            return {"message": "Bad Request"}, 400


def handleGetLimit(id, year, query):

    if query[0] == "+":
        query = int(query[1:])
        if query > 100:
            query = 100
        sql = f'Select * from countries where id={id} and date={year} order by value desc LIMIT {query}'
        res = handleCommand(sql)
        return getQ6(res)

    if query[0] == "-":
        query = int(query[1:])
        if query > 100:
            query = 100
        sql = f'Select * from countries where id={id} and date={year} order by value LIMIT {query}'
        res = handleCommand(sql)
        return getQ6(res)

    if query[0] == 1:
        sql = f'Select * from countries where id={id} and date={year} order by value LIMIT 10'
        res = handleCommand(sql)
        return getQ6(res)


def getQ6(res):

    indicator_id = f"{res[0][1]}"
    indicator_value = f"{res[0][2]}"
    entries = []

    for i in res:
        singleRecord = {}
        singleRecord["country"] = i[4]
        singleRecord["value"] = i[7]
        entries.append(singleRecord)

    return {
        "indicator": indicator_id,
        "indicator_value": indicator_value,
        "entries": entries
    }


def updateTableId(content, idValue):
    count = handleCommand("Select count(*) from countries")
    count = count[0][0]
    # print("beforeinsert:", count)
    conn = sqlite3.connect('z5177443.db')
    cursor = conn.cursor()
    content = content[1]

    dt = fetchCurrentTime()
    for i in content:
        if i["value"] != None:
            count += 1
            cursor.execute("insert into countries values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           [count, i["indicator"]["id"], i["indicator"]["value"], i["country"]["id"], i["country"]["value"], i['countryiso3code'],
                            i["date"], i["value"], i["unit"], i["obs_status"], i["decimal"], dt, idValue])
    # print("afterinsert:", count)
    conn.commit()
    conn.close()


def checkDb():
    if os.path.exists("z5177443.db"):
        return 1
    return 0


def createDb():
    try:
        if checkDb():
            return 1
        conn = sqlite3.connect('z5177443.db')
        cursor = conn.cursor()
        # Insert a row of data
        cursor.execute('''CREATE TABLE countries
                        (num real, indicator_id text, indicator_value text, country_id text, country_value real, countryiso3code real,
                        date text, value real,unit text,obs_status text,decial decimal,creation_time text,id int,PRIMARY KEY(num))
                        ''')
        conn.commit()
        conn.close()
    except:
        pass


if __name__ == '__main__':
    createDb()
    app.run(debug=True)
