import sqlite3
import pymysql
import pandas

input_connection = None

try:
    input_connection = sqlite3.connect('file:NenormLib.db?mode=rw', uri=True)
except sqlite3.OperationalError:
    print('база данных не существует')
    exit(1)

input_cursor = input_connection.cursor()

print('исходная база данных:')

sql = "select distinct reader from library"
input_cursor.execute(sql)
input_reader = input_cursor.fetchall()
print('столбец reader:', input_reader)

sql = "select distinct book, kind, author from library"
input_cursor.execute(sql)
input_book = input_cursor.fetchall()
print('столбец book:', input_book)

sql = "select distinct author from library"
input_cursor.execute(sql)
input_author = input_cursor.fetchall()
print('столбец author:', input_author)

sql = "select distinct kind from library"
input_cursor.execute(sql)
input_kind = input_cursor.fetchall()
print('столбец kind:', input_kind)

sql = "select distinct phone from library"
input_cursor.execute(sql)
input_phone = input_cursor.fetchall()
print('столбец phone:', input_phone)

sql = "select reader, book, date from library"
input_cursor.execute(sql)
input_issue = input_cursor.fetchall()
print('выдача книг:', input_issue)

input_connection.close()

print('новая база данных:')

connection = pymysql.connect(
    host="remotemysql.com",
    port=3306,
    db="VgL1MbGCiQ",
    user="VgL1MbGCiQ",
    password="ByZbnpQB8I"
)

cursor = connection.cursor()


sql = "create table readers (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "surname VARCHAR(30) NOT NULL," \
      "name VARCHAR(30) NOT NULL," \
      "middlename VARCHAR(35)," \
      "phone VARCHAR(15) NOT NULL" \
      ")"
cursor.execute(sql)

i = 0
for info in input_reader:
    SNM = info[0].split(' ')
    sql = "insert into readers (surname, name, middlename, phone) values ('%s', '%s', '%s', '%s')" \
          % (SNM[0], SNM[1], SNM[2], input_phone[i][0])
    cursor.execute(sql)
    i = i + 1

sql = "select * from readers"
cursor.execute(sql)
print('таблица readers:', cursor.fetchall())


sql = "create table kinds (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(30) NOT NULL" \
      ")"
cursor.execute(sql)

for info in input_kind:
    sql = "insert into kinds (name) values ('%s')" % info[0]
    cursor.execute(sql)

sql = "select * from kinds"
cursor.execute(sql)
print('таблица kinds:', cursor.fetchall())


sql = "create table books (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(50) NOT NULL," \
      "kind INTEGER NOT NULL," \
      "FOREIGN KEY (kind) REFERENCES kinds (id)" \
      ")"
cursor.execute(sql)

for info in input_book:
    sql = "select id from kinds where name='%s'" % (info[1])
    cursor.execute(sql)
    kind = cursor.fetchone()
    sql = "insert into books (name, kind) values ('%s', '%s')" % (info[0], kind[0])
    cursor.execute(sql)

sql = "select * from books"
cursor.execute(sql)
print('таблица books:', cursor.fetchall())


sql = "create table authors (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(100) NOT NULL" \
      ")"
cursor.execute(sql)

for info in input_author:
    sql = "insert into authors (name) values ('%s')" % info[0]
    cursor.execute(sql)

sql = "select * from authors"
cursor.execute(sql)
print('таблица authors:', cursor.fetchall())


sql = "create table book_authors (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "book INTEGER NOT NULL," \
      "author INTEGER NOT NULL," \
      "FOREIGN KEY (book) REFERENCES books (id)," \
      "FOREIGN KEY (author) REFERENCES authors (id)" \
      ")"
cursor.execute(sql)

for info in input_book:
    sql = "select id from books where name='%s'" % (info[0])
    cursor.execute(sql)
    book = cursor.fetchone()
    sql = "select id from authors where name='%s'" % (info[2])
    cursor.execute(sql)
    author = cursor.fetchone()
    sql = "insert into book_authors (book, author) values ('%s', '%s')" % (book[0], author[0])
    cursor.execute(sql)

sql = "select * from book_authors"
cursor.execute(sql)
print('таблица book_authors:', cursor.fetchall())


sql = "create table book_issue (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "issue_date DATE NOT NULL, " \
      "book INTEGER NOT NULL," \
      "reader INTEGER NOT NULL," \
      "FOREIGN KEY (book) REFERENCES books (id)," \
      "FOREIGN KEY (reader) REFERENCES readers (id)" \
      ")"
cursor.execute(sql)

for info in input_issue:
    SNM = info[0].split(' ')
    sql = "select id from readers where surname='%s' and name='%s' and middlename='%s'" % (SNM[0], SNM[1], SNM[2])
    cursor.execute(sql)
    reader = cursor.fetchone()
    sql = "select id from books where name='%s'" % (info[1])
    cursor.execute(sql)
    book = cursor.fetchone()
    sql = "insert into book_issue (issue_date, book, reader) values ('%s', '%s', '%s')" \
          % (info[2], book[0], reader[0])
    cursor.execute(sql)

sql = "select * from book_issue"
cursor.execute(sql)
print('таблица book_issue:', cursor.fetchall())


file = 'excel dump.xlsx'
writer = pandas.ExcelWriter(file, engine='xlsxwriter')

dataFrame = pandas.read_sql("select * from readers", connection)
dataFrame.to_excel(writer, 'читатели')
dataFrame = pandas.read_sql("select * from kinds", connection)
dataFrame.to_excel(writer, 'жанры')
dataFrame = pandas.read_sql("select * from books", connection)
dataFrame.to_excel(writer, 'книги')
dataFrame = pandas.read_sql("select * from authors", connection)
dataFrame.to_excel(writer, 'авторы')
dataFrame = pandas.read_sql("select * from book_authors", connection)
dataFrame.to_excel(writer, 'книга-автор')
dataFrame = pandas.read_sql("select * from book_issue", connection)
dataFrame.to_excel(writer, 'книга-читатель')

try:
    writer.save()
except Exception:
    print("ОШИБКА записи данных в файл. Возможно необходимый файл открыт в редакторе.")
    print("закройте его и попробуйте заново")
    raise

sql = "drop table book_issue"
cursor.execute(sql)
sql = "drop table book_authors"
cursor.execute(sql)
sql = "drop table authors"
cursor.execute(sql)
sql = "drop table books"
cursor.execute(sql)
sql = "drop table kinds"
cursor.execute(sql)
sql = "drop table readers"
cursor.execute(sql)

connection.close()
