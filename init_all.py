import hdf5_getters as GETTERS
import random
import cx_Oracle as oracle
import unicodedata

dataName = 'msd_summary_file.h5'
dataPath = './' + dataName

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
providers = None
genre_ids = None
customers = None

CREATE_CUSTOMER_TABLE = """
    create table CUSTOMER (
        username varchar2(20) not null,
        passwd   varchar2(20) not null,
        street   varchar2(20),
        zip      varchar2(5),
        state    varchar2(2),
        city     varchar2(15),
        country  varchar2(3),
        fname    varchar2(10) not null,
        lname    varchar2(10) not null,
        gender   varchar2(1),
        age      number check (age > 0 and age < 150),
        primary key(username)
    )
"""

CREATE_PROVIDER_TABLE = """
    create table PROVIDER (
        username varchar2(20) not null,
        passwd   varchar2(20) not null,
        street   varchar2(20),
        zip      varchar2(5),
        state    varchar2(2),
        city     varchar2(15),
        country  varchar2(3),
        company  varchar2(20) not null,
        primary key(company),
        unique(username)
    )
"""

CREATE_GENRE_TABLE = """
    create table GENRE (
        id      number,
        name    varchar2(20) not null,
    	primary key(id),
    	unique(name)
    )
"""

CREATE_SONG_TABLE = """
    create table SONG (
        id             number,
        title          varchar2(100) not null,
        avg_rate       number(2,1) check(avg_rate >= 0 and avg_rate <= 5),
        release_date   number,
        duration       number,
        price          number(3,2) not null,
        provider_name  varchar2(20),
        customer_name  varchar2(20),
        genre_id       number,
        singer_id      varchar2(50),
        primary key(id),
        foreign key(provider_name) references PROVIDER(company) on delete cascade,
        foreign key(customer_name) references CUSTOMER(username) on delete cascade,
        foreign key(genre_id) references GENRE(id) on delete cascade,
        foreign key(singer_id) references SINGER(id) on delete cascade
    )
"""

CREATE_SINGER_TABLE = """
    create table SINGER (
        id varchar2(50) not null,
        name varchar2(50) not null,
        hotness number,
        primary key(id)
    )
"""

CREATE_FAVORITE_GENRE_TABLE = """
    create table FAVORITEGENRE (
        username varchar2(20) references CUSTOMER(username),
        genre_id number references GENRE(id),
        primary key(username, genre_id)
    )
"""

CREATE_FAVORITE_SINGER_TABLE = """
    create table FAVORITESINGER (
        username varchar2(20) references CUSTOMER(username),
        singer_id varchar2(50) references SINGER(id),
        primary key(username, singer_id)
    )
"""

INSERT_CUSTOMER = """
    insert into CUSTOMER (username, passwd, street, city, state, zip, country, fname, lname, gender, age)
    values (:username, :passwd,
            :street, :city, :state, :zip, :country,
            :fname, :lname,
            :gender, :age)
"""

INSERT_PROVIDER = """
    insert into PROVIDER (username, passwd, street, city, state, zip, country, company)
    values (:username, :passwd,
            :street, :city, :state, :zip, :country,
            :company)
"""

INSERT_GENRE = """
    insert into GENRE (id, name)
    values (:id, :name)
"""

INSERT_SONG = """
    insert into SONG (id, title, avg_rate, release_date, duration, price,
    provider_name, customer_name, genre_id, singer_id)
    values (:id, unistr(:title), :avg_rate, :release_date, :duration, :price,
    :provider_name, :customer_name, :genre_id, unistr(:singer_id))
"""

INSERT_SINGER = """
    insert into SINGER (id, name, hotness)
    values (:id, :name, :hotness)
"""

INSERT_FAVORITE_GENRE = """
    insert into FAVORITEGENRE (username, genre_id)
    values (:username, :genre_id)
"""

INSERT_FAVORITE_SINGER = """
    insert into FAVORITESINGER (username, singer_id)
    values (:username, :singer_id)
"""



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

    providers = []
    genre_ids = []
    customers = []
    singers = []

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

class Provider(User):
    def __init__(self, username, passwd, company, street=None, zip=None, \
            state=None, city=None, country=None):
        super().__init__(username, passwd, street, zip, state, city, country)
        self.company = company

class Customer(User):
    def __init__(self, username, passwd, fname, lname, street=None, zip=None, \
            state=None, city=None, country=None, gender=None, age=None):
        super().__init__(username, passwd, street, zip, state, city, country)
        self.fname = fname
        self.lname = lname
        self.gender = gender
        self.age = age

def show_all_tables():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from user_tables")
    row = cursor.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()

def create_customer_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_CUSTOMER_TABLE)
    except Exception as e:
        print(e)
    finally:
        close_all(conn, cursor)

def create_provider_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_PROVIDER_TABLE)
    except Exception as e:
        print(e)
    finally:
        close_all(conn, cursor)

def create_genre_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_GENRE_TABLE)
    except Exception as e:
        print(e)
    finally:
        close_all(conn, cursor)

def create_song_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_SONG_TABLE)
    except Exception as e:
        print(e)
    finally:
        close_all(conn, cursor)

def create_singer_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_SINGER_TABLE)
    except Exception as e:
        print(e)
    close_all(conn, cursor)

def create_favoritegenre_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_FAVORITE_GENRE_TABLE)
    except Exception as e:
        print(e)
    close_all(conn, cursor)

def create_favoritesinger_table():
    conn = get_conn()
    cursor = get_cursor(conn)
    try:
        cursor.execute(CREATE_FAVORITE_SINGER_TABLE)
    except Exception as e:
        print(e)
    close_all(conn, cursor)



def insert_customer():
    print('Inserting customer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for i in range(10):
        __username = usernames[i]
        __street, __city, __state, __zip, __country = InfoGenerator.gen_address()
        __fname, __lname = InfoGenerator.gen_name()
        __gender = InfoGenerator.gen_gender()
        __age = InfoGenerator.gen_age()

        try:
            cursor.execute(INSERT_CUSTOMER,
                    username = __username, passwd = passwd,
                    street = __street, city = __city, state = __state, zip = __zip, country = __country,
                    fname = __fname, lname = __lname,
                    gender = __gender, age = __age)
            customers.append(__username)
            conn.commit()
        except Exception as e:
            print(e)
    close_all(conn, cursor)


def insert_provider():
    print('Inserting provider tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for i in range(5):
        __username = usernames[i]
        __street, __city, __state, __zip, __country = InfoGenerator.gen_address()
        __company = InfoGenerator.gen_company(i)

        try:
            cursor.execute(INSERT_PROVIDER,
                    username = __username, passwd = passwd,
                    street = __street, city = __city, state = __state, zip = __zip, country = __country,
                    company = __company)
            providers.append(__company)
            conn.commit()
        except Exception as e:
            print(e)
    close_all(conn, cursor)


def insert_genre():
    print('Inserting genre tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for i in range(len(genre_opts)):
        __id = i
        __name = InfoGenerator.gen_genre_name(i)

        try:
            cursor.execute(INSERT_GENRE,
                    id = __id, name = __name)
            genre_ids.append(__id)
            conn.commit()
        except Exception as e:
            print(e)
    close_all(conn, cursor)


def insert_song():
    print('Inserting song tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for i in range(100):
        __id = InfoGenerator.gen_int(0, 1000000)
        __title = bytes2str(GETTERS.get_title(h5, i))
        __avg_rate = 0.0
        # use int() to transform the numpy.int32 to int which is supported by Oracle
        __release_date = int(GETTERS.get_year(h5, i))
        if __release_date == 0:
            __release_date = None
        __duration = int(GETTERS.get_duration(h5, i))
        __price = InfoGenerator.gen_price()
        __provider_name = InfoGenerator.get_provider_name()
        __customer_name = InfoGenerator.get_customer_name()
        __genre_id = InfoGenerator.get_genre_id()
        __singer_id = bytes2str(GETTERS.get_artist_id(h5, i))

        try:
            cursor.execute(INSERT_SONG,
                    id = __id, title = __title, avg_rate = __avg_rate, release_date = __release_date,
                    duration = __duration, price = __price, provider_name = __provider_name,
                    customer_name = __customer_name, genre_id = __genre_id, singer_id = __singer_id)
            conn.commit()
        except Exception as e:
            print(e)
            print('i:', i, '\nid:',__id, '\ntitle:', __title, '\navg_rate:', __avg_rate,
                  '\nrelease_date:', __release_date, '\nduration', __duration, '\nprice', __price,
                  '\ngenre_id:', __genre_id, '\nsinger_id', __singer_id)
    close_all(conn, cursor)


def insert_singer():
    print('Inserting singer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)
    unique = set()
    for i in range(100):
        __name = bytes2str(GETTERS.get_artist_name(h5, i))
        if __name not in unique:
            unique.add(__name)
            __singer_id = bytes2str(GETTERS.get_artist_id(h5, i))
            __initial_hotness = 0
            try:
                cursor.execute(INSERT_SINGER,
                        id = __singer_id, name = __name,
                        hotness = __initial_hotness)
            except Exception as e:
                print(e)
    conn.commit()
    close_all(conn, cursor)


def insert_favoritegenre():
    print('Inserting favorite_genre tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for __username in customers:
        rows = 3
        unique = set()
        for i in range(rows):
            __genre_id = genre_ids[InfoGenerator.gen_int(0, len(genre_ids) - 1)]
            if __genre_id not in unique:
                unique.add(__genre_id)
                try:
                    cursor.execute(INSERT_FAVORITE_GENRE,
                         username = __username, genre_id = __genre_id)
                except Exception as e:
                    print(e)
    conn.commit()
    close_all(conn, cursor)


def insert_favoritesinger():
    print('Inserting favorite_singer tuples')
    conn = get_conn()
    cursor = get_cursor(conn)

    for __username in customers:
        rows = 3
        unique = set()
        for i in range(rows):
            index = InfoGenerator.gen_int(0, 99)
            __singer_id = bytes2str(GETTERS.get_artist_id(h5, index))
            if __singer_id not in unique:
                unique.add(__singer_id)
                try:
                    cursor.execute(INSERT_FAVORITE_SINGER,
                         username = __username, singer_id = __singer_id)
                except Exception as e:
                    print(e)
                    print('singer_id:',__singer_id)
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
    create_customer_table()
    create_provider_table()
    create_genre_table()
    create_singer_table()
    create_song_table()
    create_favoritesinger_table()
    create_favoritegenre_table()

def insert_tuples():
    insert_customer()
    insert_provider()
    insert_genre()
    insert_singer()
    insert_song()
    insert_favoritegenre()
    insert_favoritesinger()

if __name__ == '__main__':
    init()
    create_tables()
    insert_tuples()

