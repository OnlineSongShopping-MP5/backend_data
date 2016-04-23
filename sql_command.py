
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


""" TRIGGER """
TRIGGER_DOWNLOAD = """
    create or replace trigger adjust_download
     after update of paid on orders
     for each row
    declare
      prev_download number;
      singer_download number;
      singerid varchar2(20);
    begin
      select download into prev_download from song where song.id = :new.song_id;
      select song.singer_id into singerid from song where song.id = :new.song_id;
      select singer.hotness into singer_download from singer where singer.id = singerid;
    
      update song set song.download = prev_download + 1 where song.id = :new.song_id;
      update singer set singer.hotness = singer_download + 1 where singer.id = singerid;
    end;
"""

TRIGGER_AVG_RATE = """
    create or replace trigger adjust_avg_rate
     before insert on rate
     for each row
    declare
      aver number(2, 1);
      cnt number;
      total number;
    begin
      select sum(score), count(*) into total, cnt from rate where song_id = :new.song_id;
      if cnt != 0
      then
        aver := (total + :new.score) / (cnt + 1);
        update song set song.avg_rate = aver where song.id = :new.song_id;
        --dbms_output.put_line(aver);
      else
        update song set song.avg_rate = :new.score where song.id = :new.song_id;
        --dbms_output.put_line(:new.score);
      end if;
    end;
"""


""" PROCEDURE """
PROCEDURE_INSERT_CART = """
    create or replace procedure insertOrder (customer in varchar2, song_id in varchar2, orderid in varchar2)
    as
     s_price number(3, 2);
    begin
     select price into s_price from song where song.id = song_id;
     insert into orders values(customer, orderid, song_id, systimestamp, s_price, 0);
    end;
"""
