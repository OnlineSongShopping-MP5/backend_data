
CREATE_CUSTOMER_TABLE = """
    create table CUSTOMER (
        username varchar2(20),
        passwd   varchar2(20) not null,
        street   varchar2(20),
        zip      varchar2(5),
        state    varchar2(2),
        city     varchar2(15),
        country  varchar2(3),
        fname    varchar2(10) not null,
        lname    varchar2(10) not null,
        gender   varchar2(1) check (gender = 'N' or gender = 'M' or gender = 'F'),
        age      number check (age > 0 and age < 150),
        primary key(username)
    )
"""

CREATE_PROVIDER_TABLE = """
    create table PROVIDER (
        username varchar2(20) not null unique,
        passwd   varchar2(20) not null,
        street   varchar2(20),
        zip      varchar2(5),
        state    varchar2(2),
        city     varchar2(15),
        country  varchar2(3),
        company  varchar2(20),
        primary key(company)
    )
"""

CREATE_GENRE_TABLE = """
    create table GENRE (
        id      number,
        name    varchar2(20) not null unique,
    	primary key(id)
    )
"""

CREATE_SONG_TABLE = """
    create table SONG (
        id             varchar2(50),
        title          varchar2(300) not null,
        avg_rate       number(2,1) check(avg_rate >= 0 and avg_rate <= 5),
        release_date   number,
        duration       number,
        price          number(3,2) not null,
        provider_name  varchar2(20) references PROVIDER(company) on delete cascade,
        genre_id       number references GENRE(id) on delete cascade,
        singer_id      varchar2(50) references SINGER(id) on delete cascade,
        download       number,
        primary key(id)
    )
"""

CREATE_SINGER_TABLE = """
    create table SINGER (
        id varchar2(50) not null,
        name varchar2(300) not null,
        hotness number,
        primary key(id)
    )
"""

CREATE_FAVORITE_GENRE_TABLE = """
    create table FAVORITE_GENRE (
        username varchar2(20) references CUSTOMER(username) on delete cascade,
        genre_id number references GENRE(id) on delete cascade,
        primary key(username, genre_id)
    )
"""

CREATE_FAVORITE_SINGER_TABLE = """
    create table FAVORITE_SINGER (
        username varchar2(20) references CUSTOMER(username) on delete cascade,
        singer_id varchar2(50) references SINGER(id) on delete cascade,
        primary key(username, singer_id)
    )
"""

CREATE_ORDER_TABLE = """
    create table ORDERS (
        customer varchar2(20) references CUSTOMER(username) on delete cascade,
        order_id varchar2(100),
        song_id varchar2(50) references SONG(id) on delete cascade,
        time timestamp not null,
        price number(3,2) not null,
        paid number(1) default 0 not null check (paid = 0 or paid = 1),
        primary key(order_id)
    )
"""

CREATE_RATE_TABLE = """
    create table RATE (
        score number not null check(score >= 0 and score <= 5),
        customer varchar2(20) references CUSTOMER(username) on delete cascade,
        song_id varchar2(50) references SONG(id) on delete cascade,
        primary key(customer, song_id)
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
    provider_name, genre_id, singer_id, download)
    values (:id, :title, :avg_rate, :release_date, :duration, :price,
    :provider_name, :genre_id, :singer_id, :download)
"""

INSERT_SINGER = """
    insert into SINGER (id, name, hotness)
    values (:id, :name, :hotness)
"""

INSERT_FAVORITE_GENRE = """
    insert into FAVORITE_GENRE (username, genre_id)
    values (:username, :genre_id)
"""

INSERT_FAVORITE_SINGER = """
    insert into FAVORITE_SINGER (username, singer_id)
    values (:username, :singer_id)
"""

INSERT_ORDER = """
    insert into ORDERS (customer, order_id, song_id, time, price, paid)
    values (:customer, :order_id, :song_id, :time, :price, :paid)
"""

INSERT_RATE = """
    insert into RATE (score, customer, song_id)
    values (:score, :customer, :song_id)
"""

