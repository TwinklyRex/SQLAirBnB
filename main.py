import pandas as pd
import sqlite3


#Establish Connection
#Create Database

conn =sqlite3.connect("listings.db")
c=conn.cursor()

#Check if table already exists
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'listings'")
if c.fetchone()[0] == 1:
    pass
#Delete if it already exists
    c.execute("DELETE FROM listings")
else:
#Create 1NF table
    c.execute("""CREATE TABLE listings
    (id INTEGER PRIMARY KEY,
    name text,
    host_id int,
    host_name text,
    neighbourhood text,
    neighbourhood_group text, 
    longitude float,
    latitude float,
    room_type text,
    price int,
    minimum_nights int,
    number_of_reviews int,
    last_review text,
    reviews_per_month float,
    calculated_host_listings_count int,
    availability_365 int)""")
#Check if table already exists in database
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Host'")
if c.fetchone()[0] == 1:
    pass
#Delete records if it does already exist
    c.execute("DELETE FROM Host")
else:
#Create 2NF Tables
    c.execute("""CREATE TABLE Host
   (id INTEGER PRIMARY KEY,
   host_id int,
   host_name text)""")
    c.execute("")
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Room'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM Room")
else:
    c.execute("""CREATE TABLE Room
          (id INTEGER PRIMARY KEY,
        name text,
        room_type text,
        price int,
        minimum_nights int,
        availability_365 int)""")
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Reviews'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM Reviews")
else:
    c.execute("""CREATE TABLE Reviews
            (id INTEGER PRIMARY KEY, 
            number_of_reviews int,
            last_review int,
            reviews_per_month int)""")
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Location'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM Location")
else:
    c.execute("""CREATE TABLE Location
             (id INTEGER PRIMARY KEY,
             neighbourhood text,
             neighbourhood_group text,
             longitude real,
             latitude real)""")
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Expensive_Room'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM Expensive_Room")
else:
    c.execute("""CREATE TABLE Expensive_Room
           (id INTEGER PRIMARY KEY,
        name text,
        room_type text,
        price int,
        minimum_nights int,
        availability_365 int)""")
c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'neighbourhood_location'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM neighbourhood_location")
else:
    c.execute("""CREATE TABLE neighbourhood_location
              (id INTEGER PRIMARY KEY,
              neighbourhood text,
              neighbourhood_group text,
              longitude real,
              latitude real)""")

c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = 'Host_Room'")
if c.fetchone()[0] == 1:
    pass
    c.execute("DELETE FROM Host_Room")
else:
#Create 3NF Tables
    c.execute("""CREATE TABLE Host_Room
            (id INTEGER PRIMARY KEY,
            host_id INTEGER,
            FOREIGN KEY (host_id) REFERENCES Host(host_id))""")
conn.commit()

#create dataframe from dataset
listings = pd.read_csv('listings.csv')
#Check if there are duplicated records
print(sum(listings.duplicated()))
#There are none
#clean data
listings["number_of_reviews"].fillna(listings["number_of_reviews"].mean(), inplace = True)
listings["reviews_per_month"].fillna(listings["reviews_per_month"].mean(), inplace = True)
listings["last_review"].fillna("No Date", inplace = True)


#Populate 1NF Table
listings.to_sql("listings", conn, if_exists='append', index=False)


#Populate 2NF Tables
c.execute("""
INSERT INTO Host (id, host_id, host_name)
SELECT listings.id, listings.host_id, listings.host_name
FROM listings""")


c.execute("""
INSERT INTO Room(id, name, room_type, price, minimum_nights, availability_365)
SELECT listings.id, listings.name, listings.room_type, listings.price, listings.minimum_nights, listings.availability_365
FROM listings""")

c.execute("""
INSERT INTO Reviews(id, number_of_reviews, last_review, reviews_per_month)
SELECT listings.id, listings.number_of_reviews, listings.last_review, listings.reviews_per_month
FROM listings""")

c.execute("""
INSERT INTO Location(id, neighbourhood, neighbourhood_group, longitude, latitude)
SELECT listings.id, listings.neighbourhood, listings.neighbourhood_group, listings.longitude, listings.latitude
FROM listings""")

c.execute("""
INSERT INTO Expensive_Room(id, name, room_type, price, minimum_nights, availability_365)
SELECT listings.id, listings.name, listings.room_type, listings.price, listings.minimum_nights, listings.availability_365
FROM listings
WHERE listings.price >= 100""")

c.execute("""
INSERT INTO neighbourhood_location(id, neighbourhood, neighbourhood_group, longitude, latitude)
SELECT listings.id, listings.neighbourhood, listings.neighbourhood_group, listings.longitude, listings.latitude
FROM listings
WHERE neighbourhood_group = 'Central Region'""")

#Populate 3NF Table
c.execute("""
INSERT INTO Host_Room(id, host_id)
SELECT listings.id, listings.host_id
FROM listings""")
conn.commit()


class host():
    def __init__(self, id, host_id, host_name):
        self.id = id
        self.host_id = host_id
        self.host_name = host_name
    def get_id(self):
        return self.id
    def get_host_id(self):
        return self.host_id
    def get_host_name(self):
        return self.host_name
    def read_host(self):
        c.execute("SELECT * FROM Host")
        print(c.fetchall())
    def uhost(self):
        return 'You have updated this table'
    def dhost(self):
        return 'You have deleted a record from this table'
    def ahost(self):
        return 'You have added a record to this table'

#host test
#instantiate a class instance
hst = host(324234, 12345, "Marcus Rashford")
#print(hst.get_id())
#print(hst.get_host_name())
#print(hst.get_host_id())
#print(hst.read_host())


class update_host(host):
    def __init__(self):
        c.execute("""
                UPDATE Host
                SET id = 38112762, host_id = 28788521, host_name = 'Bruno Fernandes'
                WHERE id = 38112762;""")
        conn.commit()

#Test update_host
#instantiate a class instance

print(update_host())
#print(hst.read_host())


class delete_host(host):
    def __init__(self):
        c.execute("""
        DELETE FROM Host
        WHERE host_id = 243835202""")
    conn.commit()
#Test delete_host
#delete_host()
#hst.read_host()


class add_host(host):
    def __init__(self):

        host1 = (
            (4323421111, 2432424, "Luke Shaw"),
            (2222312414, 6546464, "Alex Telles")
        )
        host2 = [
            [765975697, 2422345436, "Amad Diallo"],
            [765898989, 3537687875, "Mason Greenwood"]
        ]
        host3 = [
            (2243524523, 876578652, "Paul Pogba"),
            (1123414234, 413351543, "Nemanja Vidic")
        ]

        c.executemany("insert into Host(id, host_id, host_name) values(?, ?, ?)", host1)
        c.executemany("insert into Host(id, host_id, host_name) values(?, ?, ?)", host2)
        c.executemany("insert into Host(id, host_id, host_name) values(?, ?, ?)", host3)
        print('Records inserted successfully.')

#Test add_host
add_host()
#hst.read_host()

class room ():
    def __init__(self, id, name, room_type, price, minimum_nights, availability_365):
        self.id = id
        self.name = name
        self.room_type = room_type
        self.price = price
        self.minimum_nights = minimum_nights
        self.availability_365 = availability_365
    def get_id(self):
        return self.id
    def get_name(self):
        return self.name
    def get_room_type(self):
        return self.room_type
    def get_price(self):
        return self.price
    def get_minimum_nights(self):
        return self.minimum_nights
    def get_availability_365(self):
        return self.availability_365
    def read_room(self):
        c.execute("SELECT * FROM Room")
        print(c.fetchall())

#room test
rm = room(43232435, "Shrute Farms", "Entire House/Apt", 120, 1, 365)
#print(rm.get_id())
#print(rm.get_name())
#print(rm.get_room_type())
#print(rm.get_price())
#print(rm.get_minimum_nights())
#print(rm.get_availability_365())
#rm.read_room()



class update_room(room):
    def __init__(self):
        c.execute("""
                UPDATE Room
                SET id = 38112762, name = 'The Coffee House', room_type = 'Entire home/apt', price = 140, minimum_nights = 3,availability_365 = 365
                WHERE id = 38112762;""")
        conn.commit()

#Test update_host
#update_room()
#print(rm.read_room())


class delete_room(room):
    def __init__(self):
        c.execute("""
        DELETE FROM Room
        WHERE id = 38110493""")
    conn.commit()
#Test delete_host
#delete_room()
#print(rm.read_room())

class add_room(room):
    def __init__(self):

        room1 = (
            (4323421111, "The Theatre of Dreams", "Private Room", 1000, 1, 365),
            (2222312414, "The Humble Abode", "Private Room", 10, 10, 250)
        )
        room2 = [
            [765975697, "The Yellow Submarine", "Private Room", 99, 2, 365],
            [765898989, "Hotel California", "Entire House/Apt", 220, 5, 290]
        ]
        room3 = [
            (2243524523, "Club Tropicana", "Entire House/Apt",   354, 59, 300),
            (1123414234, "Our House", "Entire House/Apt", 70, 7, 365)
        ]

        c.executemany("insert into Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)", room1)
        c.executemany("insert into Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)", room2)
        c.executemany("insert into Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)", room3)
        print('Records inserted successfully.')

#Test add_host
#add_room()
#rm.read_room()

class reviews():
    def __init__(self, id, number_of_reviews, last_review, reviews_per_month):
        self.id = id
        self.number_of_reviews = number_of_reviews
        self.last_review = last_review
        self.reviews_per_month = reviews_per_month
    def get_id(self):
        return self.id
    def get_number_of_reviews(self):
        return self.number_of_reviews
    def get_last_review(self):
        return self.last_review
    def get_reviews_per_month(self):
        return self.reviews_per_month
    def order_reviews(self):
        c.execute("""SELECT * FROM Reviews
        ORDER BY number_of_reviews""")
        print(c.fetchall())
    def read_reviews(self):
        c.execute("SELECT * FROM Reviews")
        print(c.fetchall())


#reviews test
rvw = reviews(658586, 324, "20/02/2020", 0.43)
print(rvw.get_id())
print(rvw.get_reviews_per_month())
print(rvw.get_last_review())
print(rvw.get_number_of_reviews())
#rvw.order_reviews()
#print(rvw.read_reviews())

class update_reviews(reviews):
    def __init__(self):
        c.execute("""
                UPDATE Reviews
                SET id = 38112762, number_of_reviews = 20, last_review = '03/06/2019', reviews_per_month = 0.11
                WHERE id = 38112762;""")
        conn.commit()

#Test update_host
#update_reviews()
#print(rvw.read_reviews())


class delete_reviews(reviews):
    def __init__(self):
        c.execute("""
        DELETE FROM Reviews
        WHERE id = 38110493""")
    conn.commit()
#Test delete_host
#delete_reviews()
#print(rvw.read_reviews())

class add_reviews(reviews):
    def __init__(self):

        review1 = (
            (4323421111, 444, "11/11/2021", 2),
            (2222312414, 342, "12/12/13", 5)
        )
        review2 = [
            [765975697, 9324, "12/10/21", 12],
            [765898989, 324, "22/10/21", 0.33]
        ]
        review3 = [
            (2243524523, 10, "30/11/13", 0.1),
            (1123414234, 50, "25/10/17", 1)
        ]

        c.executemany("insert into Reviews(id, number_of_reviews, last_review, reviews_per_month) values(?, ?, ?, ?)", review1)
        c.executemany("insert into Reviews(id, number_of_reviews, last_review, reviews_per_month) values(?, ?, ?, ?)", review2)
        c.executemany("insert into Reviews(id, number_of_reviews, last_review, reviews_per_month) values(?, ?, ?, ?)", review3)
        print('Records inserted successfully.')

#Test add_reviews
#add_reviews()
#rvw.read_reviews()


class location:
    def __init__(self, id, neighbourhood_group, neighbourhood, latitude, longitude):
        self.id = id
        self.neighbourhood_group = neighbourhood_group
        self.neighbourhood = neighbourhood
        self.latitude = latitude
        self.longitude = longitude
    def get_id(self):
        return self.id
    def get_neighbourhood_group(self):
        return self.neighbourhood_group
    def get_neighbourhood(self):
        return self.neighbourhood
    def get_latitude(self):
        return self.latitude
    def get_longitude(self):
        return self.longitude
    def view_neighbourhood(self):
        c.execute("""SELECT * FROM location
        GROUP BY neighbourhood""")
        print(c.fetchall())
    def view_neighbourhood_group(self):
        c.execute("""SELECT * FROM location
        GROUP BY neighbourhood_group""")
        print(c.fetchall())
    def read_location(self):
        c.execute("SELECT * FROM location")
        print(c.fetchall())

#location test
lc = location(39242, "EAST", "Stranmillis", 1.2323, 32.3423)
#print(lc.get_id())
#print(lc.get_neighbourhood_group())
#print(lc.get_neighbourhood())
#print(lc.get_latitude())
#print(lc.get_longitude())
#lc.view_neighbourhood()
#lc.view_neighbourhood_group()
#print(lc.read_location())

class update_location(location):
    def __init__(self):
        c.execute("""
                UPDATE location
                SET id = 38112762, neighbourhood = 'Queenstown', neighbourhood_group = 'North-East Region', latitude = 104.34545, longitude = 1.43542
                WHERE id = 38112762;""")
        conn.commit()

#Test update_host
#update_location()
#print(lc.read_location())


class delete_location(location):
    def __init__(self):
        c.execute("""
        DELETE FROM location
        WHERE id = 38110493""")
    conn.commit()
#Test delete_host
#delete_location()
#print(lc.read_location())

class add_location(location):
    def __init__(self):
        location1 = (
            (4323421111, "Central Region", "Queenstown", 1.57483, 102.8349),
            (2222312414, "North-East Region", "Tanglin", 1.23943, 103.3429)
        )
        location2 = [
            [765975697, "North Region", "Downtown Core", 1.11492, 104.8549],
            [765898989, "Central Region", "Tanglin", 1.12321, 102.9384]
        ]
        location3 = [
            (2243524523, "West Region", "Kallang", 1.23323, 101.2329),
            (1123414234, "North-East Region", "Punggol", 1.34657, 103.4840)
        ]

        c.executemany("insert into location(id, neighbourhood_group, neighbourhood, latitude, longitude) values(?, ?, ?, ?, ?)",
                      location1)
        c.executemany("insert into location(id, neighbourhood_group, neighbourhood, latitude, longitude) values(?, ?, ?, ?, ?)",
                      location2)
        c.executemany("insert into location(id, neighbourhood_group, neighbourhood, latitude, longitude) values(?, ?, ?, ?, ?)",
                      location3)
        print('Records inserted successfully.')


#Test add_location
#add_location()
#lc.read_location()

class Expensive_Room (room):
    def __init__(self):
        room.__init__(self, 9999999, "Luxury Room", "Entire home/apt", 230, 4, 365)
    def get_id(self):
        return self.id
    def get_name(self):
        return self.name
    def get_room_type(self):
        return self.room_type
    def get_price(self):
        return self.price
    def get_minimum_nights(self):
        return self.minimum_nights
    def get_availability_365(self):
        print(self.availability_365)
    def low_to_high(self):
        c.execute("""SELECT * FROM Expensive_Room
        ORDER BY price""")
        print(c.fetchall())
    def order_mnights(self):
        c.execute("""SELECT * FROM Expensive_Room
        ORDER BY minimum_nights""")
        print(c.fetchall())
    def read_expensive_room(self):
        c.execute("SELECT * FROM Expensive_Room")
        print(c.fetchall())

#Test Expensive Room
er = Expensive_Room()
#print(er.get_id())
#print(er.get_name())
#print(er.get_room_type())
#print(er.get_price())
#print(er.get_minimum_nights())
#print(er.get_availability_365())
er.order_mnights()
#print(er.low_to_high())
#print(er.read_expensive_room())

class max_revenue(room):
    def __init__(self):
        room.__init__(self, 9999999, "Luxury Room", "Entire home/apt", 10, 4, 365)
    def max_r(self):
        return self.price * self.availability_365
#Test max_revenue
mr = max_revenue()
#print(mr.max_r())

class update_expensive_room(Expensive_Room):
    def __init__(self):
        c.execute("""
                UPDATE Expensive_Room
                SET id = 38108273, name = 'Spacious 1 Bedroom Apt', room_type = 'Private room', price = 160, minimum_nights = 10, availability_365 = 90
                WHERE id = 38108273;""")
        conn.commit()


#Test update_host
#update_expensive_room()
#print(er.read_expensive_room())


class delete_expensive_room(Expensive_Room):
    def __init__(self):
        c.execute("""
        DELETE FROM Expensive_Room
        WHERE id = 38105126""")
    conn.commit()
#Test delete_host
#delete_expensive_room()
#print(er.read_expensive_room())

class add_expensive_room(Expensive_Room):
    def __init__(self):
        er1 = (
            (4323421111, "White Room", "Private Room", 100, 3, 365),
            (2222312414, "Versailles", "Entire House/Apt", 180, 2, 300)
        )
        er2 = [
            [765975697, "Mount Saint-Michel", "Entire House/Apt", 120, 1, 365],
            [765898989, "Downing Street", "Private Room", 199, 7, 365]
        ]
        er3 = [
            (2243524523, "White House", "Private Room", 400, 5, 250),
            (1123414234, "Big Blue House", "Private Room", 100, 3, 100)
        ]
        c.executemany(
            "insert into Expensive_Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)",
            er1)
        c.executemany(
            "insert into Expensive_Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)",
            er2)
        c.executemany(
            "insert into Expensive_Room(id, name, room_type, price, minimum_nights, availability_365) values(?, ?, ?, ?, ?, ?)",
            er3)
        print('Records inserted successfully.')

# Test add_expensive_room
#add_expensive_room()
#er.read_expensive_room()

class neighbourhood_location(location):
    def __init__(self):
        location.__init__(self, 39242, "EAST", "Stranmillis", 1.2323, 32.3423)
    def get_id(self):
        return self.id
    def get_neighbourhood_group(self):
        return self.neighbourhood_group
    def get_neighbourhood(self):
        return self.neighbourhood
    def get_latitude(self):
        return self.latitude
    def get_longitude(self):
        return self.longitude
    def read_neighbourhood_location(self):
        c.execute("SELECT * FROM neighbourhood_location")
        print(c.fetchall())

#location test
nl = neighbourhood_location()
#print(lc.get_id())
#print(lc.get_neighbourhood_group())
#print(lc.get_neighbourhood())
#print(lc.get_latitude())
#print(lc.get_longitude())
#print(nl.read_neighbourhood_location())