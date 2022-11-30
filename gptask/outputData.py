#!/usr/bin/env python

import mysql.connector
import json


print("Starting outputdata..")

##region db conn
mydb = mysql.connector.connect(
    host="database",
    user="codetest",
    password="swordfish",
    database="codetest"
)
#
##endregion
print("connecting to the database..")
cursor = mydb.cursor()

print("processing data..")

sql = """
SELECT c.country_name, COUNT(p.id) AS total
    FROM country c
    JOIN county c2 ON c2.country_id = c.id
    JOIN city c3 ON c3.county_id = c2.id
    JOIN person p ON p.city_id = c2.id
GROUP BY c.country_name"""

cursor.execute(sql)
result = cursor.fetchall()
listJs = []
for r in result:
    jObject = {"country": r[0],
               "total": r[1]}
    #print(jObject)
    listJs.append(jObject)

resultObject = {"result": listJs}
resultJson = json.dumps(resultObject)


print(resultJson)

json_file_path = "/data/outputResult.json"

with open(json_file_path, 'w', encoding="utf-8") as writeFile:
    json.dump(resultObject, writeFile)

print("json result saved at /Data")
