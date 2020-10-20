import sqlite3
import pymysql
import pandas


class Database:
    def __init__(self, dbname):
        self.dbname = dbname
        self.connection = None
        self.my_cursor = None

    def disconnect(self):
        try:
            self.connection.close()
        except AttributeError:
            print('ОШИБКА: подключение к базе данных уже отсутствует')
            exit(1)
        except Exception:
            raise
        self.connection = None
        self.my_cursor = None

    def execute_query(self, query, rows_return_type='fetchall'):
        try:
            self.my_cursor.execute(query)
        except Exception:
            raise
        if rows_return_type == 'fetchone':
            return self.my_cursor.fetchone()
        else:
            return self.my_cursor.fetchall()

    def cursor(self):
        return self.connection.cursor()


class SQLite_Database(Database):
    def __init__(self, dbname):
        super().__init__(dbname)
        self.mode = None

    @staticmethod
    def valid_mode(mode):
        if mode not in ['ro', 'rw']:
            return 'ro'
        else:
            return mode

    def connect(self, mode):
        self.mode = self.valid_mode(mode)
        try:
            self.connection = sqlite3.connect('file:%s.db?mode=%s' % (self.dbname, self.mode), uri=True)
        except sqlite3.OperationalError:
            print('ОШИБКА: база данных не существует')
            exit(1)
        except Exception:
            raise
        self.my_cursor = self.connection.cursor()

    def disconnect(self):
        super().disconnect()
        self.mode = None

    def execute_query(self, query, rows_return_type='fetchall'):
        return super().execute_query(query, rows_return_type)

    def cursor(self):
        return super().cursor()


class MySQL_Database(Database):
    def __init__(self, dbname):
        super().__init__(dbname)

    def connect(self, host, port, user, password):
        try:
            self.connection = pymysql.connect(
                host=host,
                port=port,
                db=self.dbname,
                user=user,
                password=password
            )
        except Exception:
            raise
        self.my_cursor = self.connection.cursor()

    def disconnect(self):
        super().disconnect()

    def execute_query(self, query, rows_return_type='fetchall'):
        return super().execute_query(query, rows_return_type)

    def cursor(self):
        return super().cursor()


class Reader:
    def __init__(self, SNM=None, phone=None):
        self.SNM = SNM
        self.phone = phone

    def convertDB(self, mysql_obj):
        i = 0
        for info in self.SNM:
            split_SNM = info[0].split(' ')
            sql = "insert into readers (surname, name, middlename, phone) values ('%s', '%s', '%s', '%s')" \
                  % (split_SNM[0], split_SNM[1], split_SNM[2], self.phone[i][0])
            MySQL_Database.execute_query(mysql_obj, sql)
            i = i + 1


class Kind:
    def __init__(self, name=None):
        self.name = name

    def convertDB(self, mysql_obj):
        for info in self.name:
            sql = "insert into kinds (name) values ('%s')" % info[0]
            MySQL_Database.execute_query(mysql_obj, sql)


class Book:
    def __init__(self, data=None):
        self.data = data

    def convertDB(self, mysql_obj):
        for info in self.data:
            sql = "select id from kinds where name='%s'" % (info[1])
            kind = MySQL_Database.execute_query(mysql_obj, sql, 'fetchone')
            sql = "insert into books (name, kind) values ('%s', '%s')" % (info[0], kind[0])
            MySQL_Database.execute_query(mysql_obj, sql)


class Author:
    def __init__(self, name=None):
        self.name = name

    def convertDB(self, mysql_obj):
        for info in self.name:
            sql = "insert into authors (name) values ('%s')" % info[0]
            MySQL_Database.execute_query(mysql_obj, sql)


class BookAuthors:
    def __init__(self, data=None):
        self.data = data

    def convertDB(self, mysql_obj):
        for info in self.data:
            sql = "select id from books where name='%s'" % (info[0])
            book = MySQL_Database.execute_query(mysql_obj, sql, 'fetchone')
            sql = "select id from authors where name='%s'" % (info[2])
            author = MySQL_Database.execute_query(mysql_obj, sql, 'fetchone')
            sql = "insert into book_authors (book, author) values ('%s', '%s')" % (book[0], author[0])
            MySQL_Database.execute_query(mysql_obj, sql)


class BookIssue:
    def __init__(self, data=None):
        self.data = data

    def convertDB(self, mysql_obj):
        for info in self.data:
            split_SNM = info[0].split(' ')
            sql = "select id from readers where surname='%s' and name='%s' and middlename='%s'" % (
                split_SNM[0], split_SNM[1], split_SNM[2])
            reader = MySQL_Database.execute_query(mysql_obj, sql, 'fetchone')
            sql = "select id from books where name='%s'" % (info[1])
            book = MySQL_Database.execute_query(mysql_obj, sql, 'fetchone')
            sql = "insert into book_issue (issue_date, book, reader) values ('%s', '%s', '%s')" \
                  % (info[2], book[0], reader[0])
            MySQL_Database.execute_query(mysql_obj, sql)


class ExcelWriter:
    def __init__(self, filename, dbconn):
        self.file = filename
        self.writer = pandas.ExcelWriter(self.file, engine='xlsxwriter')
        self.connection = dbconn

    def write_to_excel(self, sql, listname):
        try:
            dataFrame = pandas.read_sql(sql, self.connection)
            dataFrame.to_excel(self.writer, listname)
        except Exception:
            raise
        print("don't forget to save all write_to_excel() executions results by calling save_changes() method")

    def save_changes(self):
        try:
            self.writer.save()
        except Exception:
            print("ОШИБКА записи данных в файл. Возможно необходимый файл открыт в редакторе.")
            print("Закройте его и попробуйте заново")
            raise


print('исходная база данных:')
input_connection = SQLite_Database('NenormLib')
input_connection.connect('rw')

input_reader = Reader()
sql = "select distinct reader from library"
input_reader.SNM = input_connection.execute_query(sql)
print('столбец reader:', input_reader.SNM)

sql = "select distinct phone from library"
input_reader.phone = input_connection.execute_query(sql)
print('столбец phone:', input_reader.phone)

input_kind = Kind()
sql = "select distinct kind from library"
input_kind.name = input_connection.execute_query(sql)
print('столбец kind:', input_kind.name)

input_book = Book()
sql = "select distinct book, kind, author from library"  # не доработано
input_book.data = input_connection.execute_query(sql)
print('столбец book:', input_book.data)

input_book_authors = BookAuthors()
input_book_authors.data = input_connection.execute_query(sql)

input_author = Author()
sql = "select distinct author from library"
input_author.name = input_connection.execute_query(sql)
print('столбец author:', input_author.name)

input_issue = BookIssue()
sql = "select reader, book, date from library"
input_issue.data = input_connection.execute_query(sql)
print('выдача книг:', input_issue.data)

input_connection.disconnect()

print('новая база данных:')
output_connection = MySQL_Database("VgL1MbGCiQ")
output_connection.connect("remotemysql.com", 3306, "VgL1MbGCiQ", "ByZbnpQB8I")

sql = "drop table book_issue"
output_connection.execute_query(sql)
sql = "drop table book_authors"
output_connection.execute_query(sql)
sql = "drop table authors"
output_connection.execute_query(sql)
sql = "drop table books"
output_connection.execute_query(sql)
sql = "drop table kinds"
output_connection.execute_query(sql)
sql = "drop table readers"
output_connection.execute_query(sql)


sql = "create table readers (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "surname VARCHAR(30) NOT NULL," \
      "name VARCHAR(30) NOT NULL," \
      "middlename VARCHAR(35)," \
      "phone VARCHAR(15) NOT NULL" \
      ")"
output_connection.execute_query(sql)

input_reader.convertDB(output_connection)
sql = "select * from readers"
print('таблица readers:', output_connection.execute_query(sql))

sql = "create table kinds (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(30) NOT NULL" \
      ")"
output_connection.execute_query(sql)

input_kind.convertDB(output_connection)
sql = "select * from kinds"
print('таблица kinds:', output_connection.execute_query(sql))

sql = "create table books (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(50) NOT NULL," \
      "kind INTEGER NOT NULL," \
      "FOREIGN KEY (kind) REFERENCES kinds (id)" \
      ")"
output_connection.execute_query(sql)

input_book.convertDB(output_connection)
sql = "select * from books"
print('таблица books:', output_connection.execute_query(sql))

sql = "create table authors (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "name VARCHAR(100) NOT NULL" \
      ")"
output_connection.execute_query(sql)

input_author.convertDB(output_connection)
sql = "select * from authors"
print('таблица authors:', output_connection.execute_query(sql))

sql = "create table book_authors (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "book INTEGER NOT NULL," \
      "author INTEGER NOT NULL," \
      "FOREIGN KEY (book) REFERENCES books (id)," \
      "FOREIGN KEY (author) REFERENCES authors (id)" \
      ")"
output_connection.execute_query(sql)

input_book_authors.convertDB(output_connection)
sql = "select * from book_authors"
print('таблица book_authors:', output_connection.execute_query(sql))

sql = "create table book_issue (" \
      "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT," \
      "issue_date DATE NOT NULL, " \
      "book INTEGER NOT NULL," \
      "reader INTEGER NOT NULL," \
      "FOREIGN KEY (book) REFERENCES books (id)," \
      "FOREIGN KEY (reader) REFERENCES readers (id)" \
      ")"
output_connection.execute_query(sql)

input_issue.convertDB(output_connection)
sql = "select * from book_issue"
print('таблица book_issue:', output_connection.execute_query(sql))

dump_to_Excel = ExcelWriter('excel dump.xlsx', output_connection)
dump_to_Excel.write_to_excel("select * from readers", 'читатели')
dump_to_Excel.write_to_excel("select * from kinds", 'жанры')
dump_to_Excel.write_to_excel("select * from books", 'книги')
dump_to_Excel.write_to_excel("select * from authors", 'авторы')
dump_to_Excel.write_to_excel("select * from book_authors", 'книга-автор')
dump_to_Excel.write_to_excel("select * from book_issue", 'книга-читатель')
dump_to_Excel.save_changes()


sql = "drop table book_issue"
output_connection.execute_query(sql)
sql = "drop table book_authors"
output_connection.execute_query(sql)
sql = "drop table authors"
output_connection.execute_query(sql)
sql = "drop table books"
output_connection.execute_query(sql)
sql = "drop table kinds"
output_connection.execute_query(sql)
sql = "drop table readers"
output_connection.execute_query(sql)

output_connection.disconnect()
