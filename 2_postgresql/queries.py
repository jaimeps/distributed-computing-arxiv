# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project

import psycopg2, psycopg2.extras
import time
import pandas as pd

# Replace with AWS RDS details
# DB_DSN = "host=XX.XX.us-west-1.rds.amazonaws.com dbname=XX user=XX password=XX"

''' GENERIC QUERY FUNCTION ================================================='''
def query_arg(sql, arg):
    """ Generic function to query the database
    :param: sql: a postgres query
            arg: a tuple with the argument to pass to the query
    :return: a DataFrame with the ouput of the query """
    start = time.time()
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql, arg)
        rs = pd.DataFrame(cur.fetchall())
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
    elapsed = time.time() - start
    print 'Elapsed time: ', elapsed
    return elapsed, rs

''' QUERIES ================================================================'''
class queries_obj(object):
    def __init__(self):
        pass

    def num_words(self):
        query_string = '''SELECT words, count(words) as cnt
                FROM (
                    SELECT url, subject
                    FROM meta
                    WHERE subject = %s 
                    ) as LHS
                JOIN
                    (SELECT url, unnest(words) as words
                    FROM words) as RHS
                ON LHS.url = RHS.url
                GROUP BY words
                ORDER BY cnt desc
                LIMIT 20;'''
        return query_string

    def most_active_authors(self):
        query_string = '''SELECT authors, count(authors) as cnt
                    FROM
                        (SELECT unnest(authors) as authors from authors) as Q1
                    GROUP BY authors
                    ORDER BY cnt DESC
                    LIMIT 10;'''
        return query_string

    def avg_authors(self):
        query_string = '''SELECT avg(array_length(authors, 1)) FROM authors;'''
        return query_string

    def author_words_10(self):
        query_string = '''SELECT authors, words, cnt FROM
            (SELECT authors, words, count(*) as cnt
            FROM
                (SELECT LHS.url, authors, words
                FROM
                    (SELECT url, unnest(authors) as authors
                    FROM authors) as LHS
                JOIN
                    (SELECT url, unnest(words) as words
                    FROM words) as RHS
                ON LHS.url = RHS.url) as inner_q
            GROUP BY 1, 2
            ) as outer_q
        where cnt > 10
        LIMIT 20;'''
        return query_string



    def tf_idf(self):
        query_string = '''SELECT distinct LHS.words2, tf * log(2.0, 1256826 / df::numeric) as tfidf
            FROM
                (SELECT url, words2, count(words2) as tf
                FROM (SELECT url, unnest(words) as words2 FROM words) as LQ1
                GROUP BY url, words2) as LHS
            JOIN
                (SELECT words2, count(words2) as df
                FROM (SELECT url, unnest(words) as words2 FROM words) RQ1
                GROUP BY words2) as RHS
            ON LHS.words2 = RHS.words2
            ORDER by tfidf
            LIMIT 20;'''
        return query_string

''' MAIN SCRIPT ============================================================='''
if __name__ == '__main__':

    queries = queries_obj()
    query_times = []


    print "******* Most common words per subject **********"

    print '(1) Stats'
    elapsed, output = query_arg(queries.num_words(), ('stat',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output
    #
    print '(2) CS'
    elapsed, output = query_arg(queries.num_words(), ('cs',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output
    #
    print '(3) Math'
    elapsed, output = query_arg(queries.num_words(), ('math',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output

    print '(4) Physics'
    elapsed, output = query_arg(queries.num_words(), ('physics',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output

    print '(5) Q-bio'
    elapsed, output = query_arg(queries.num_words(), ('q-bio',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output

    print '(6) Q-fin'
    elapsed, output = query_arg(queries.num_words(), ('q-fin',))
    query_times.append(elapsed)
    # output.columns = ['word', 'count']
    print output

    print "******* Most active authors **********"

    elapsed, output = query_arg(queries.most_active_authors(), None)
    query_times.append(elapsed)
    output.columns = ['author', 'count']
    print output

    print "******* Average number of authors per paper **********"

    elapsed, output = query_arg(queries.avg_authors(), None)
    query_times.append(elapsed)
    output.columns = ['Average']
    print output

    print "******* Author's words appearing more than 10 times **********"

    elapsed, output = query_arg(queries.author_words_10(), None)
    query_times.append(elapsed)
    output.columns = ['author', 'word', 'count']
    print output

    print query_times

    print "******* TF-IDF **********"

    elapsed, output = query_arg(queries.tf_idf(), None)
    query_times.append(elapsed)
    output.columns = ['word', 'tf_idf']
    print output

    print query_times

''' ========================================================================='''

