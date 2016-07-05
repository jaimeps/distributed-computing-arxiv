# Students: A. Romriell, D. Wen, J. Pastor, J. Pollard
# MSAN 694 Project

from pyspark import SparkContext
from pyspark.mllib.clustering import PowerIterationClustering


def parse_meta(line):
	"""
	"""
	pieces = line.strip().split("|")
	return {"id": pieces[0], "subj": pieces[1], "dt": pieces[2], "title": pieces[3]}


def get_paper_subj(d):
	"""
	"""
	return (d["id"], d["subj"])


def parse_words(line):
	"""
	"""
	paper_id, the_words = line.strip().split(":", 1)
	words = the_words.strip().split("|")
	return {"id": paper_id, "words": words}


def get_id_words_kv(r):
	"""
	"""
	return (r["id"], r["words"])


def int_paper_map(d):
	"""
	"""
	tpl_list = list()
	for k in d.keys():
		t = (d[k], k)
		tpl_list.append(t)
	return tpl_list


def compute_jaccard(l1, l2):
	"""
	"""
	s1 = set(l1)
	s2 = set(l2)
	top = len(s1.intersection(s2))
	bot = len(s1.union(s2))
	return float(top) / float(bot)


def result_to_tpl(d):
	"""
	"""
	tpl = (d.id, d.cluster)
	return tpl


def words_nodes_clusters_to_csv(d):
	"""
	"""
	word_str = u"{}|{}|{}".format(d[0], d[1], d[2])
	return word_str


def words_edges_to_csv(d):
	"""
	"""
	word_str = u"{}|{}|{}".format(d[0], d[1], d[2])
	return word_str


if __name__ == '__main__':

	META = "s3://XX/paper_meta.txt"
	WORD = "s3://XX/paper_words.txt"

	numParts = 80

	sc = SparkContext(appName="WordsNetwork")

	raw_meta = sc.textFile(META, minPartitions=numParts)
	raw_word = sc.textFile(WORD, minPartitions=numParts)

	meta = raw_meta.map(parse_meta)
	words = raw_word.map(parse_words)
	id_words = words.map(get_id_words_kv)
	subj = meta.map(get_paper_subj)

	meta_math = meta.map(lambda d: (d["id"], d["subj"])).filter(lambda x: x[1] == u"math")

	uniq_papers = meta_math.map(lambda d: d[0]).distinct()
	uniq_papers_int = uniq_papers.zipWithIndex().collectAsMap()
	int_paper = sc.parallelize(int_paper_map(uniq_papers_int), numSlices=numParts)
	paper_int = int_paper.map(lambda x: (x[1], x[0]))

	intID_words = paper_int.join(id_words, numPartitions=numParts).map(lambda x: (x[1][0], x[1][1]))

	pairs = intID_words.cartesian(intID_words)

	sims = pairs.map(lambda x: (x[0][0], x[1][0], compute_jaccard(x[0][1], x[1][1])))
	sims = sims.filter(lambda x: x[2] < 1.0)

	subj_cluster_mdl = PowerIterationClustering.train(sims, 5, 25)

	result = sc.parallelize(sorted(subj_cluster_mdl.assignments().collect(), key=lambda x: x.id), numSlices=numParts)
	result = result.map(result_to_tpl)

	words_net_clust = int_paper.join(result, numPartitions=numParts).map(lambda d: (d[1][0], d[1][1]))

	words_net_clust_subj = words_net_clust.join(subj, numPartitions=numParts).map(lambda d: (d[0], d[1][1], d[1][0]))

	words_edges = sims.filter(lambda x: x[2] > 0.0)

	words_edges_names = words_edges.map(lambda d: (d[0], (d[1], d[2])))
	words_edges_names = int_paper.join(words_edges_names, numPartitions=numParts).map(lambda d: (d[1][1][0], (d[1][0], d[1][1][1])))
	words_edges_names = int_paper.join(words_edges_names, numPartitions=numParts).map(lambda d: (d[1][1][0], d[1][0], d[1][1][1]))

	words_nodes_out = "s3://XX/words_nodes"
	words_nodes_lines = words_net_clust_subj.map(words_nodes_clusters_to_csv)
	words_nodes_lines.coalesce(25).saveAsTextFile(words_nodes_out)

	words_edges_out = "s3://XX/words_edges"
	words_edges_lines = words_edges_names.map(words_edges_to_csv)
	words_edges_lines.coalesce(25).saveAsTextFile(words_edges_out)


# end of words







