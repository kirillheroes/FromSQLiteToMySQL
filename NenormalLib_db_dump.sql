PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE library (
id INTEGER PRIMARY KEY,
reader TEXT,
book TEXT,
author TEXT,
kind TEXT,
phone TEXT,
date TEXT);
INSERT INTO library VALUES(1,'Кудрявцев Максим Николаевич','Война и Мир. Том 1','Л.Н. Толстой','роман-эпопея','89554477839','2010-05-10');
INSERT INTO library VALUES(2,'Ошев Кирилл Владимирович','Преступление и наказание','Ф.М. Достоевский','психологический роман','89504547777','2010-07-15');
INSERT INTO library VALUES(3,'Ошев Кирилл Владимирович','На дне','Максим Горький','пьеса','89504547777','2010-07-30');
INSERT INTO library VALUES(4,'Ханов Дамир Радикович','Горе от ума','А.С. Грибоедов','комедия','80101010101','2010-08-02');
INSERT INTO library VALUES(5,'Кудрявцев Максим Николаевич','Война и Мир. Том 2','Л.Н. Толстой','роман-эпопея','89554477839','2010-08-02');
INSERT INTO library VALUES(6,'Ханов Дамир Радикович','Недоросль','Д.И. Фонвизин','пьеса','80101010101','2010-08-05');
INSERT INTO library VALUES(7,'Ханов Дамир Радикович','Анна Каренина','Л.Н. Толстой','роман','80101010101','2011-01-27');
INSERT INTO library VALUES(8,'Кудрявцев Максим Николаевич','Война и Мир. Том 3','Л.Н. Толстой','роман-эпопея','89554477839','2011-02-19');
COMMIT;