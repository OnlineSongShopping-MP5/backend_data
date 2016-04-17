import hdf5_getters as GETTERS
import random
import cx_Oracle as oracle
import unicodedata
import sql_command as sql
import hardcode_var as hard
import sys
from datetime import datetime

dataName = 'msd_summary_file.h5'
dataPath = '../' + dataName

USER_NAME = 'ph2'
PASSWD = 'Hp162162'
HOST = 'oracle.cise.ufl.edu'
PORT = '1521'
SID = 'orcl'

usernames = []
passwd = '12345'

h5 = None

'Options used to automatically generate tuples'
street_opts = []
state_opts = []
city_opts = []
country_opts = []
first_name1_opts = []
first_name2_opts = []
last_name_opts = []
company_opts = []
genre_opts = []

'Records of options selected'
providers = []
genre_ids = []
customers = {}
songs = {}
orders = {}

def strip_accents(str):
    return unicodedata.normalize('NFKD', str).encode('ascii','ignore')

def bytes2str(bytes):
    return strip_accents(bytes.decode('utf-8')).decode('utf-8')

def init():
    global h5, usernames, passwd
    global street_opts, state_opts, city_opts, country_opts
    global first_name1_opts, first_name2_opts, last_name_opts
    global company_opts
    global genre_opts
    global providers, customers, genre_ids, singers

    h5 = GETTERS.open_h5_file_read(dataPath)
    usernames = ['user' + str(i).zfill(4) for i in range(1000)]
    passwd = '12345'
    street_opts = ['4000 SW' + str(i).zfill(3) for i in range(1, 101)]
    state_opts = [None, 'FL', 'CA', 'WA', 'AZ', 'NY']
    city_opts = [None, 'Gainesville', 'New York', 'Austin', 'Washinton', 'Seattle']
    country_opts = [None, 'US', 'CH', 'CN', 'JP', 'MX']
    first_name1_opts = ['Mary', 'Amy', 'Ben', 'Jim', 'Ken', 'Sam', 'Kim', 'Bob', 'Alice', 'Tom']
    first_name2_opts = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']
    last_name_opts = ['Li', 'Lee', 'Park', 'Snade', 'Snake', 'Wu', 'JK', 'Timblake', 'Mat', 'Green']
    company_opts = ['Sony', 'Rock', 'Rolling stone', 'Alee', 'Park']
    genre_opts = ['Alternative', 'Anime', 'Blues', 'Classical', 'Country', 'Rap']


def get_conn():
    conn = oracle.connect(USER_NAME, PASSWD, oracle.makedsn(HOST, PORT, SID))
    return conn

def get_cursor(conn):
    return conn.cursor()

def close_all(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()

class User:
    # username, passwd, street, zip, state, city, country
    def __init__(self, username, passwd, street, zip, state, city, country):
        self.username = username
        self.passwd = passwd
        self.street = street
        self.zip = zip
        self.state = state
        self.city = city
        self.country = country
    def get_username(self):
        return self.username

class Provider(User):
    def __init__(self, username, passwd, company, street=None, zip=None,
            state=None, city=None, country=None):
        super().__init__(username, passwd, street, zip, state, city, country)
        self.company = company

class Customer(User):
    def __init__(self, username, passwd, fname, lname, street=None, zip=None,
            state=None, city=None, country=None, gender=None, age=None):
        super().__init__(username, passwd, street, zip, state, city, country)
        self.fname = fname
        self.lname = lname
        self.gender = gender
        self.age = age

class Song:
    def __init__(self, id, title, avg_rate, release_date, duration,
                 price, provider_name, genre_id, singer_id, download):
        self.id = id
        self.title = title
        self.avg_rate = avg_rate
        self.release_date = release_date
        self.duration = duration
        self.price = price
        self.provider_name = provider_name
        self.genre_id = genre_id
        self.singer_id = singer_id
        self.download = download

    def get_id(self):
        return self.id
    def get_title(self):
        return self.title
    def get_price(self):
        return self.price

class Order:
    def __init__(self, id, time, price):
        self.id = id
        self.time = time
        self.price = price

    def get_price(self):
        return self.price

    def get_time(self):
        return self.time

def show_all_tables():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from user_tables")
    row = cursor.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()

def create_customer_table():
    print('Creating CUSTOMER table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_CUSTOMER_TABLE)
        return 0
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_provider_table():
    print('Creating PROVIDER table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_PROVIDER_TABLE)
        return 0
    except Exception as e:
        print(e, 'create provider table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_genre_table():
    print('Creating GENRE table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_GENRE_TABLE)
        return 0
    except Exception as e:
        print(e, 'create genre table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_song_table():
    print('Creating SONG table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_SONG_TABLE)
        return 0
    except Exception as e:
        print(e, 'create song table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_singer_table():
    print('Creating SINGER table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_SINGER_TABLE)
        return 0
    except Exception as e:
        print(e, 'create singer table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_favorite_genre_table():
    print('Creating FAVORITE_GENRE table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_FAVORITE_GENRE_TABLE)
        return 0
    except Exception as e:
        print(e, 'create favorite_genre table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_favorite_singer_table():
    print('Creating FAVORITE_SINGER table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_FAVORITE_SINGER_TABLE)
        return 0
    except Exception as e:
        print(e, 'create favorite_singer table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_order_table():
    print('Creating ORDER table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_ORDER_TABLE)
        return 0
    except Exception as e:
        print(e, 'create order table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def create_rate_table():
    print('Creating RATE table')
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(sql.CREATE_RATE_TABLE)
        return 0
    except Exception as e:
        print(e, 'create rate table error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)



def insert_customer():
    print('Inserting customer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    try:
        for i in range(hard.NUM_CUSTOMER):
            __username = usernames[i]
            __street, __city, __state, __zip, __country = InfoGenerator.gen_address()
            __fname, __lname = InfoGenerator.gen_name()
            __gender = InfoGenerator.gen_gender()
            __age = InfoGenerator.gen_age()

            cursor.execute(sql.INSERT_CUSTOMER,
                    username = __username, passwd = passwd,
                    street = __street, city = __city, state = __state, zip = __zip, country = __country,
                    fname = __fname, lname = __lname, gender = __gender, age = __age)
            customers[i] = Customer(__username, passwd,
                    __street, __city, __state, __zip, __country,
                    __fname, __lname, __gender, __age)
        return 0
    except Exception as e:
        print(e, 'insert customer tuple error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_provider():
    print('Inserting provider tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    try:
        for i in range(hard.NUM_PROVIDER):
            __username = usernames[i]
            __street, __city, __state, __zip, __country = InfoGenerator.gen_address()
            __company = InfoGenerator.gen_company(i)

            cursor.execute(sql.INSERT_PROVIDER,
                    username = __username, passwd = passwd,
                    street = __street, city = __city, state = __state, zip = __zip, country = __country,
                    company = __company)
            providers.append(__company)
        return 0
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_genre():
    print('Inserting genre tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    __id = None
    __name = None

    try:
        for i in range(len(genre_opts)):
            __id = i
            __name = InfoGenerator.gen_genre_name(i)

            cursor.execute(sql.INSERT_GENRE,
                    id = __id, name = __name)
            genre_ids.append(__id)
        return 0
    except Exception as e:
        print(e, 'insert genre tuple error')
        print('id:',__id, 'name:',__name)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_song():
    print('Inserting song tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    __id = None
    __title = None
    __avg_rate = None
    __release_date = None
    __duration = None
    __price = None
    __provider_name = None
    __genre_id = None
    __singer_id = None
    __download = None

    try:
        for i in range(hard.NUM_SONGS):
            __id = bytes2str(GETTERS.get_song_id(h5, i))
            __title = bytes2str(GETTERS.get_title(h5, i))
            __avg_rate = 0.0
            # use int() to transform the numpy.int32 to int which is supported by Oracle
            __release_date = int(GETTERS.get_year(h5, i))
            if __release_date == 0:
                __release_date = None
            __duration = int(GETTERS.get_duration(h5, i))
            __price = InfoGenerator.gen_price()
            __provider_name = InfoGenerator.get_provider_name()
            __genre_id = InfoGenerator.get_genre_id()
            __singer_id = bytes2str(GETTERS.get_artist_id(h5, i))
            __download = 0

            cursor.execute(sql.INSERT_SONG,
                    id = __id, title = __title, avg_rate = __avg_rate, release_date = __release_date,
                    duration = __duration, price = __price, provider_name = __provider_name,
                    genre_id = __genre_id, singer_id = __singer_id, download = __download)
            songs[i] = Song(__id, __title, __avg_rate, __release_date, __duration, __price,
                               __provider_name, __genre_id, __singer_id, __download)
        return 0
    except Exception as e:
        print(e)
        print('i:', i, '\nid:',__id, '\ntitle:', __title, '\navg_rate:', __avg_rate,
              '\nrelease_date:', __release_date, '\nduration', __duration, '\nprice', __price,
              'provider_name:',__provider_name, '\ngenre_id:', __genre_id, '\nsinger_id', __singer_id,
              'download:',__download)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_singer():
    print('Inserting singer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    unique = set()
    __name = None
    __singer_id = None
    __initial_hotness = None

    try:
        for i in range(hard.NUM_SINGERS):
            __name = bytes2str(GETTERS.get_artist_name(h5, i))
            if __name not in unique:
                unique.add(__name)
                __singer_id = bytes2str(GETTERS.get_artist_id(h5, i))
                __initial_hotness = 0
                cursor.execute(sql.INSERT_SINGER,
                        id = __singer_id, name = __name,
                        hotness = __initial_hotness)
        return 0
    except Exception as e:
        print(e, 'insert singer tuple error')
        print('name:',__name, 'singer_id:',__singer_id, 'hotness:',__initial_hotness)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_favorite_genre():
    print('Inserting favorite_genre tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    try:
        for customer in customers.values():
            rows = 3
            unique = set()
            for i in range(rows):
                __genre_id = genre_ids[InfoGenerator.gen_int(0, len(genre_ids) - 1)]
                if __genre_id not in unique:
                    unique.add(__genre_id)
                    cursor.execute(sql.INSERT_FAVORITE_GENRE,
                    username = customer.get_username(), genre_id = __genre_id)
        return 0
    except Exception as e:
        print(e, 'insert favorite_genre tuple error')
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_favorite_singer():
    print('Inserting favorite_singer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    __singer_id = None

    try:
        for customer in customers.values():
            rows = 3
            unique = set()
            for i in range(rows):
                index = InfoGenerator.gen_int(0, 99)
                __singer_id = bytes2str(GETTERS.get_artist_id(h5, index))
                if __singer_id not in unique:
                    unique.add(__singer_id)
                    cursor.execute(sql.INSERT_FAVORITE_SINGER,
                            username = customer.get_username(), singer_id = __singer_id)
        return 0
    except Exception as e:
        print(e, 'insert favorite_singer tuple error')
        print('singer_id:',__singer_id)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)


def insert_order():
    print('Inserting order tuples')
    conn = get_conn()
    cursor = get_cursor(conn)
    __customer = None
    __order_id = None
    __song_id = None
    __time = None
    __price = None
    __paid = None

    try:
        for i in range(hard.NUM_ORDER):
            index_song = InfoGenerator.gen_int(0, len(songs) - 1)
            __customer = customers[InfoGenerator.gen_int(0, len(customers) - 1)].get_username()
            __order_id = i
            __song_id = songs[index_song].get_id()
            __price = songs[index_song].get_price()
            __time = datetime.now()
            __paid = InfoGenerator.gen_int(0, 1)

            cursor.execute(sql.INSERT_ORDER,
                    customer = __customer, order_id = __order_id, song_id = __song_id,
                    time = __time, price = __price, paid = __paid)
        return 0
    except Exception as e:
        print(e, 'insert order tuple error')
        print('customer:',__customer, 'order_id:',__order_id, 'song_id:',__song_id,
                'time:',__time, 'price:',__price, 'paid:',[False,True][__paid])
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

def insert_rate():
    print('Inserting rate tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    __customer = None
    __song_id = None
    __score = None

    try:
        for customer in customers.values():
            index_song = InfoGenerator.gen_int(0, len(songs) - 1)
            __customer = customer.get_username()
            __song_id = songs[index_song].get_id()
            __score = InfoGenerator.gen_int(0, 5)

            cursor.execute(sql.INSERT_RATE,
                    score = __score, customer = __customer, song_id = __song_id)
        return 0
    except Exception as e:
        print(e, 'insert rate tuple error')
        print('customer:',__customer, 'song_id:',__song_id, 'score:',__score)
        return -1
    finally:
        conn.commit()
        close_all(conn, cursor)

class InfoGenerator:
    @staticmethod
    def gen_int(start, end):
        return random.randint(start, end)

    # 1.23
    @staticmethod
    def gen_float(start, end):
        return random.uniform(start, end)

    @staticmethod
    def gen_genre_name(i):
        return genre_opts[i % len(genre_opts)]

    @staticmethod
    def gen_company(i):
        return company_opts[i % len(company_opts)]

    @staticmethod
    def gen_address():
        street = street_opts[InfoGenerator.gen_int(0, 99)]
        zip = str(InfoGenerator.gen_int(0, 99999)).zfill(5)
        state = state_opts[InfoGenerator.gen_int(0, 5)]
        city = city_opts[InfoGenerator.gen_int(0, 5)]
        country = country_opts[InfoGenerator.gen_int(0, 5)]
        return street, city, state, zip, country

    @staticmethod
    def gen_age():
        return InfoGenerator.gen_int(5, 70)

    @staticmethod
    def gen_gender():
        return ['M', 'N', 'F'][InfoGenerator.gen_int(0, 2)]

    @staticmethod
    def gen_name():
        fname = first_name1_opts[InfoGenerator.gen_int(0, 9)] + ' ' + first_name2_opts[InfoGenerator.gen_int(0, 9)]
        lname = last_name_opts[InfoGenerator.gen_int(0, 9)]
        return fname, lname

    @staticmethod
    def gen_price():
        return float("%.2f" % InfoGenerator.gen_float(0.0, 9.99))

    @staticmethod
    def get_provider_name():
        return providers[InfoGenerator.gen_int(0, len(providers) - 1)]

    @staticmethod
    def get_customer_name():
        return customers[InfoGenerator.gen_int(0, len(customers) - 1)]

    @staticmethod
    def get_singer_id():
        return singers[InfoGenerator.gen_int(0, len(singers) - 1)]

    @staticmethod
    def get_genre_id():
        return genre_ids[InfoGenerator.gen_int(0, len(genre_ids) - 1)]

def create_tables():
    if (create_customer_table()
    or create_provider_table()
    or create_genre_table()
    or create_singer_table()
    or create_song_table()
    or create_favorite_singer_table()
    or create_favorite_genre_table()
    or create_order_table()
    or create_rate_table()):
        sys.exit(0)

def insert_tuples():
    if (insert_customer()
    or insert_provider()
    or insert_genre()
    or insert_singer()
    or insert_song()
    or insert_favorite_genre()
    or insert_favorite_singer()
    or insert_order()
    or insert_rate()):
        sys.exit(0)

if __name__ == '__main__':
    init()
    create_tables()
    insert_tuples()

