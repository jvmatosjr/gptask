# This is a sample Python script.
import mysql.connector
import pandas as pd
import mysql.connector

##region ddl
ddl_person ="""
CREATE TABLE person(
	id INT NOT NULL AUTO_INCREMENT,
	given_name VARCHAR(100) CHARACTER SET UTF8MB4,
	family_name VARCHAR(100) CHARACTER SET UTF8MB4,
	date_of_birth DATE,
	place_of_birth VARCHAR(100) CHARACTER SET UTF8MB4,
	city_id INT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY (city_id)
		REFERENCES city(id)
);
"""
ddl_country = """
CREATE TABLE country(
	id INT NOT NULL AUTO_INCREMENT,
	country_name VARCHAR(100) CHARACTER SET UTF8MB4,
	PRIMARY KEY (id)
);"""
ddl_county = """
CREATE TABLE county(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	county_name VARCHAR(100) CHARACTER SET UTF8MB4,
	country_id INT,
	INDEX idx_country(country_id), 
	FOREIGN KEY (country_id)
		REFERENCES country(id)
);"""
ddl_city = """
CREATE TABLE city(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	city_name VARCHAR(100) CHARACTER SET UTF8MB4,
	county_id INT,
	INDEX idx_country(county_id), 
	FOREIGN KEY (county_id)
		REFERENCES county(id)
);"""
##endregion
peoplePath = '/home/jvmj/projects/gotphoto/data/people.csv'
placesPath = '/home/jvmj/projects/gotphoto/data/places.csv'

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

##region read db objects
def getCountries():
    sql = "select id, country_name from country"
    cursor.execute(sql)
    countries = [{"id": row[0], "country_name": row[1]} for row in cursor.fetchall()]
    return countries

def getCounties():
    sql = """SELECT c1.id, county_name, country_name 
                FROM county c1
            JOIN country c2 ON c2.id = c1.country_id"""
    cursor.execute(sql)
    counties = [{"id": row[0], "county_name": row[1], "country_name": row[2]} for row in cursor.fetchall()]
    return counties

def getCities():
    sql = """SELECT id, city_name 
                FROM city"""
    cursor.execute(sql)
    cities = [{"id": row[0], "city_name": row[1]} for row in cursor.fetchall()]
    return cities
##endregion

##region get ids
def getCountyId(country, county, counties):
    county_id = 0
    for c in counties:
        if(c["country_name"] == country and c["county_name"] == county):
            county_id = c["id"]
            break
    return county_id

def getCityId(city, cities):
    city_id = None;
    for c in cities:
        if(c["city_name"] == city):
            city_id = c["id"]
            break
    return city_id

def getCountryId(country, countries):
    country_id = 0
    for c in countries:
        if(c["country_name"] == country):
            country_id = c["id"]
            break
    return country_id

##endregion

##region inserts
def insertCountry(list):
    for item in list:
        sql = "INSERT INTO country(country_name) VALUES('{}')"
        sql = sql.format(item)
        #print(sql)
        cursor.execute(sql)

def insertCity(cities):
    #get country Ids
    counties = getCounties()
    for idx, row in cities.iterrows():
        county_id = getCountyId(row["country"],row["county"], counties)
        #print(country_id)
        if county_id == 0:
            print("county_id not found.")
            break;
        sql = "INSERT INTO city(city_name, county_id) VALUES('{}',{})"
        sql = sql.format(row["city"], county_id)
        #print(sql)
        cursor.execute(sql)

def insertCounty(counties):
    #get county Ids
    countries = getCountries()
    for idx, row in counties.iterrows():
        country_id = getCountryId(row["country"], countries)
        #print(country_id)
        if country_id == 0:
            print("country not found.")
            break;
        sql = "INSERT INTO county(county_name, country_id) VALUES('{}',{})"
        sql = sql.format(row["county"], country_id)
        #print(sql)
        cursor.execute(sql)

def insertPerson(people):
    #get city Ids
    cities = getCities()
    #print(cities)
    for idx, row in people.iterrows():
        #print(row["place_of_birth"])
        city_id = getCityId(row["place_of_birth"], cities)
        #print(country_id)
        if city_id == None:
            print("city not found.")
        specialName = row["family_name"];
        specialName = specialName.replace('\'','\'\'')
        sql = "INSERT INTO person(given_name, family_name, date_of_birth, place_of_birth, city_id) VALUES('{}','{}','{}','{}',{})"
        sql = sql.format(row["given_name"],specialName,row["date_of_birth"],row["place_of_birth"], city_id)
        #print(sql)
        cursor.execute(sql)
##endregion

##region load functions
def loadCountries(data):
    countries = pd.Series(data["country"]).unique()
    countriesdf = pd.DataFrame(countries, columns=['country_name'])
    insertCountry(countries)
    mydb.commit()
    print('countries inserted.')

def loadCounties(data):
    counties = data[["county","country"]].copy()
    counties = counties.drop_duplicates()
    #print(counties)
    insertCounty(counties)
    mydb.commit()
    print('counties inserted.')

def loadCities(data):
    #cities = data[["city","county","country"]].drop_duplicates()
    cities = data.drop_duplicates()
    insertCity(cities)
    mydb.commit()
    print('cities inserted.')

def loadPlaces(path):
    df = pd.read_csv(path, delimiter=',', encoding='UTF-8', header=0)
    loadCountries(df)
    loadCounties(df)
    loadCities(df)

def loadPeople(path):
    df = pd.read_csv(path, delimiter=',', encoding='UTF-8', header=0)
    insertPerson(df)
    mydb.commit()
    print('people inserted.')
##endregion



def initTables():
    #drop all tables if exists
    sql = "DROP TABLE IF EXISTS person "
    cursor.execute(sql)
    print("drop tables if exists")
    sql = "DROP TABLE IF EXISTS city "
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS county "
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS country "
    cursor.execute(sql)
    mydb.commit()

    #create tables fresh
    print("create tables")
    cursor.execute(ddl_country);
    mydb.commit()
    cursor.execute(ddl_county);
    mydb.commit()
    cursor.execute(ddl_city);
    mydb.commit()
    cursor.execute(ddl_person);
    mydb.commit()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #initTables()
    #loadPlaces(placesPath)
    #loadPeople(peoplePath)
    print("test docker")

