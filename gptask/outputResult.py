# This is a sample Python script.
import mysql.connector
import pandas as pd
import mysql.connector
import json

##region db conn
mydb = mysql.connector.connect(
    host="localhost",
    user="codetest",
    password="swordfish",
    database="codetest"
)
#
##endregion
cursor = mydb.cursor()

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
    #print(r)
    jObject = {"country": r[0],
               "total": r[1]}
    #print(jObject)
    listJs.append(jObject)

resultObject = {"result:": listJs}
#jList = json.dumps(listJs)
resultJson = json.dumps(resultObject)
#print(jList)
print(resultJson)

