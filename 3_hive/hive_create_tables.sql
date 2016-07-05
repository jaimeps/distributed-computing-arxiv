DROP table IF EXISTS paper_meta;

CREATE table paper_meta (
    url_id STRING,
    subject STRING,
    dt STRING,
    title STRING)
ROW FORMAT delimited
    fields terminated by '|'
STORED as textfile;

LOAD data inpath '/data/paper_meta.txt'
OVERWRITE into table paper_meta;

/* 
----------------------- LOCAL VAGRANT file load ------------------------
LOAD data local inpath '/home/vagrant/DistComp_DATA/paper_meta10000.txt'
OVERWRITE into table paper_meta;
------------------------------------------------------------------------
*/

/* 
************************************************************************
*************---------- partitioning by subject------------*************
************************************************************************
*/ 

DROP table IF EXISTS paper_meta_tmp;

CREATE table paper_meta_tmp (
    url_id STRING,
    subject STRING,
    dt STRING,
    title STRING)
ROW FORMAT delimited
    fields terminated by '|'
STORED as textfile;

LOAD data inpath '/data/paper_meta.txt'
OVERWRITE into table paper_meta_tmp;

/* 
-- then insert into partitioned table by select '...' where subject = '...'
*/ 

DROP table IF EXISTS table paper_meta;

CREATE table paper_meta (
    url_id STRING,
    dt STRING,
    title STRING)
PARTITIONED BY (subject STRING)
ROW FORMAT delimited
    fields terminated by '|'
STORED as textfile;

INSERT OVERWRITE table paper_meta PARTITION(subject='cs')
SELECT url_id, dt, title
FROM paper_meta_tmp
WHERE subject = 'cs';

/* 
====================================================================
*/ 

DROP table IF EXISTS words_tmp;

CREATE EXTERNAL table words_tmp (
    url_id STRING,
    word ARRAY <STRING>)
ROW FORMAT delimited
    fields terminated by ':'
    collection items terminated by '|'
STORED as textfile;

LOAD data inpath '/data/paper_words.txt'
OVERWRITE into table words_tmp;

/* 
----------------------- LOCAL VAGRANT file load ------------------------
LOAD data local inpath '/home/vagrant/DistComp_DATA/paper_words10000.txt'
OVERWRITE into table words_tmp;
------------------------------------------------------------------------
*/ 

DROP table if EXISTS words;

CREATE table words (url_id STRING, word STRING);

INSERT into table words
SELECT url_id, word_element
FROM words_tmp
LATERAL VIEW EXPLODE(word) tmp_tbl AS word_element;

/* 
====================================================================
*/ 

DROP table IF EXISTS authors_tmp;

CREATE table authors_tmp (
    url_id STRING,
    author ARRAY <STRING>)
ROW FORMAT delimited
    fields terminated by ':'
    collection items terminated by '|'
STORED as textfile;

LOAD data inpath '/data/parsed_authors.txt'
OVERWRITE into table authors_tmp;


DROP table if EXISTS authors;

CREATE table authors (url_id STRING, author STRING);

INSERT into table authors
SELECT url_id, author_element
FROM authors_tmp
LATERAL VIEW EXPLODE(author) tmp_tbl AS author_element;

/* 
----------------------- LOCAL VAGRANT file load ------------------------
LOAD data local inpath '/home/vagrant/DistComp_DATA/parsed_authors10000.txt'
OVERWRITE into table authors_tmp;
------------------------------------------------------------------------
*/ 
