# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project

import psycopg2, psycopg2.extras
from psycopg2.extensions import AsIs
import codecs
import time

# Replace with AWS RDS details
DB_DSN = "host=XX.XX.us-west-1.rds.amazonaws.com dbname=XX user=XX password=XX"

# Replace with directory to parsed files
PATH = '/arxiv_data/'

INPUT_DATA_1 = 'parsed_authors.txt'
INPUT_DATA_2 = 'paper_words.txt'
INPUT_DATA_3 = 'paper_meta.txt'


''' CREATE_TABLE FUNCTION ==================================================='''
def create_table(sql):
    """ creates a postgres table
    :return: """
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

''' DROP_TABLE FUNCTION ====================================================='''
def drop_table(table_name):
    """ drops the table if it exists
    :param table_name: string with table name
    :return: """
    try:
        sql = "DROP TABLE IF EXISTS %(table)s;"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql, {"table":AsIs(table_name)})
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()


''' INSERT_DATA FUNCTION ============================================'''
def insert_data(path, file_txt, table_name, delimiter = ':'):
    """ Inserts fast the data
    :param  path: directory of the txt file
            file_txt: txt file with parsed data
            table_name: table where to insert the data
    :return: """
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        io = codecs.open(path + file_txt, 'r', encoding = 'UTF-8')
        cur.copy_from(io, table_name, sep=delimiter)
        conn.commit()
        io.close()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

''' PARSE_TXT_1 FUNCTION ==============================================='''
def parse_txt_1(file_input, file_output, path):
    """
    :param  file_input: name of txt file with the papers and authors
            file_output: name of txt to create
            path: directory of both files
    :return:
    """
    f_txt1 = codecs.open(path + file_input, encoding='UTF-8')
    f_txt2 = codecs.open(path + file_output, 'w', encoding='UTF-8')
    for line in f_txt1:
        url, array = line.split(':', 1)
        array = array.replace(':', '').replace('||', '|')
        array = array.replace('\\', '').strip('|')
        array = '{"' + array.replace('"', '').rstrip("\r\n") + '"}'
        array = array.replace('|', '","')
        f_txt2.write(url + ':' + array + '\n')
    f_txt1.close()
    f_txt2.close()

''' PARSE_TXT_2 FUNCTION ==============================================='''
def parse_txt_2(file_input, file_output, path):
    """
    :param  file_input: name of txt file with the papers and authors
            file_output: name of txt to create
            path: directory of both files
    :return:
    """
    f_txt1 = codecs.open(path + file_input, encoding='UTF-8')
    f_txt2 = codecs.open(path + file_output, 'w', encoding='UTF-8')
    for line in f_txt1:
        url, title = line.split('|', 1)
        subject, title = title.split('|', 1)
        dt, title = title.split('|', 1)
        title = title.replace('\\', '')
        f_txt2.write(url.strip() + '|' + subject + '|' + dt + '|' +
                     title.replace('|', ''))
    f_txt1.close()
    f_txt2.close()


''' MAIN SCRIPT ============================================================='''
if __name__ == '__main__':

    start = time.time()
    parse_txt_authors(INPUT_DATA_1, 'temp.txt', PATH)
    print "******* Preparing data **********"

    print "******* Dropping table **********"
    drop_table("authors")
    #
    print "******* Creating table **********"
    sql1 = "CREATE TABLE authors (url VARCHAR, authors TEXT[]);"
    create_table(sql1)

    print "******* Inserting data into table **********"
    insert_data(PATH, 'temp.txt', 'authors')

    end = time.time()
    print 'Elapsed time: ', end - start

    start = time.time()
    print "******* Preparing data **********"
    parse_txt_1(INPUT_DATA_2, 'temp.txt', PATH)

    print "******* Dropping table **********"
    drop_table("words")

    print "******* Creating table **********"
    sql2 = "CREATE TABLE words (url VARCHAR, words TEXT[]);"
    create_table(sql2)

    print "******* Inserting data into table **********"
    insert_data(PATH, 'temp.txt', 'words')

    end = time.time()
    print 'Elapsed time: ', end - start

    start = time.time()
    print "******* Preparing data **********"
    parse_txt_2(INPUT_DATA_3, 'temp.txt', PATH)
    #
    print "******* Dropping table **********"
    drop_table("meta")

    print "******* Creating table **********"
    sql3 = "CREATE TABLE meta (url VARCHAR, subject VARCHAR, dt DATE, " \
           " title VARCHAR);"
    create_table(sql3)

    print "******* Inserting data into table **********"
    insert_data(PATH, 'temp.txt', 'meta', '|')
    #
    end = time.time()
    print 'Elapsed time: ', end - start

''' ========================================================================='''
