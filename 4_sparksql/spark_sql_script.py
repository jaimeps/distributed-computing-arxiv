# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project


import queries_sparksql
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


def create_tables(list_files, n_repartition):

    # TABLE AUTHORS
    # Read authors txt
    lines = sc.textFile(list_files[0])
    parts = lines.map(lambda l: l.split(":", 1))
    authors = parts.map(lambda p: Row(url=p[0], authors=p[1].split('|')))

    # Register the SchemaRDD as a table
    df_authors = authors.toDF()
    df_authors.registerTempTable("authors")

    # Put table in cache for reuse, and repartition
    df_authors.cache()
    df_authors = df_authors.repartition(n_repartition)

    # TABLE WORDS
    lines = sc.textFile(list_files[1])
    parts = lines.map(lambda l: l.split(":", 1))
    words = parts.map(lambda p: Row(url=p[0], words=p[1].split('|')))

    # Register the SchemaRDD as a table
    df_words = words.toDF()
    df_words.registerTempTable("words")

    # Put table in cache for reuse, and repartition
    df_words.cache()
    df_words = df_words.repartition(n_repartition)

    # TABLE META
    lines = sc.textFile(list_files[2])
    parts = lines.map(lambda l: l.split("|"))
    meta = parts.map(lambda p: Row(url=p[0], subject=p[1], dt=p[2], title=p[3]))

    # Register the SchemaRDD as a table
    df_meta = meta.toDF()
    df_meta.registerTempTable("meta")

    # Put table in cache for reuse, and repartition
    df_meta.cache()
    df_meta = df_meta.repartition(n_repartition)

def explode_tables():
    # TABLE AUTHORS2
    query_string = '''SELECT url, explode(authors) as authors
                        FROM authors'''
    query = sqlContext.sql(query_string)
    query.registerTempTable("authors2")

    # TABLE WORDS2
    query_string = '''SELECT url, explode(words) as words
                        FROM words'''
    query = sqlContext.sql(query_string)
    query.registerTempTable("words2")

def query_collect(query_string):
    start = time.time()
    query = sqlContext.sql(query_string)
    for item in query.collect():
        print item
    elapsed = time.time() - start
    print 'Elapsed time: ', elapsed
    return elapsed



''' MAIN SCRIPT ============================================================='''
if __name__ == '__main__':

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    n_repartition = 6

    list_files = [
        "s3://XX/data/parsed_authors.txt",
        "s3://XX/data/paper_words.txt",
        "s3://XX/paper_meta.txt"]

    create_tables(list_files, n_repartition)
    explode_tables()

    queries = queries_sparksql.queries_obj()
    query_times = []

    print "******* Most common words per subject **********"

    print '(1) Stats'
    query_string = queries.num_words('stat')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '(2) CS'
    query_string = queries.num_words('cs')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '(3) Math'
    query_string = queries.num_words('math')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '(4) Physics'
    query_string = queries.num_words('physics')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '(5) Q-bio'
    query_string = queries.num_words('q-bio')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '(6) Q-fin'
    query_string = queries.num_words('q-fin')
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print "******* Most active authors **********"
    query_string = queries.most_active_authors()
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print "******* Average number of authors per paper **********"
    query_string = queries.avg_authors()
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print "******* Author's words appearing more than 10 times **********"
    query_string = queries.author_words_10()
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print "******* TF-IDF **********"
    query_string = queries.tf_idf()
    elapsed = query_collect(query_string)
    query_times.append(elapsed)

    print '\n\nRESULT:'
    print query_times
    print '\n\n'

    sc.stop()
