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


def parse_auth(line):
	"""
	"""
	paper_id, the_authors = line.strip().split(":", 1)
	authors = the_authors.strip().split("|")
	return {"id": paper_id, "authors": authors}


def id_auth_ct(d):
	"""
	"""
	return {"id": d["id"], "num_auth": len(d["authors"])}


def int_auth_map(d):
	"""
	"""
	tpl_list = list()
	for k in d.keys():
		t = (d[k], k)
		tpl_list.append(t)
	return tpl_list


def auth_pairs(d, auth_ints):
	"""
	"""
	n = len(d["authors"])
	auth_pairs = list()
	if n > 1:
		for i in range(n):
			for j in range(i+1, n):
				tpl = (auth_ints[d["authors"][i]], auth_ints[d["authors"][j]])
				auth_pairs.append(tpl)
	# else:
	# 	tpl = (d["authors"][0], )
	# 	auth_pairs.append(tpl)
	return auth_pairs


def auth_pairs_ct(d):
	"""
	"""
	return (d, 1)


def auth_ct_to_three_tpl(d):
	"""
	"""
	tpl = (d[0][0], d[0][1], d[1])
	return tpl


def result_to_tpl(d):
	"""
	"""
	tpl = (d.id, d.cluster)
	return tpl


def auths_edges_to_csv(d):
	"""
	"""
	auth_str = u"{}|{}|{}".format(d[0], d[1], d[2])
	return auth_str


def auths_nodes_clusters_to_csv(d):
	"""
	"""
	auth_str = u"{}|{}".format(d[0], d[1])
	return auth_str



if __name__ == '__main__':

	META = "s3://XX/paper_meta.txt"
	AUTH = "s3://XX/parsed_authors.txt"

	numParts = 100

	sc = SparkContext(appName="AuthorNetwork")

	raw_meta = sc.textFile(META, minPartitions=numParts)
	raw_auth = sc.textFile(AUTH, minPartitions=numParts)

	meta = raw_meta.map(parse_meta)
	auths = raw_auth.map(parse_auth)

	uniq_auths = auths.flatMap(lambda d: d["authors"]).distinct()
	uniq_auths_int = uniq_auths.zipWithIndex().collectAsMap()

	int_auth = sc.parallelize(int_auth_map(uniq_auths_int), numSlices=numParts)

	authID_pairs = auths.flatMap(lambda x: auth_pairs(x, uniq_auths_int))

	auth_ct = authID_pairs.map(auth_pairs_ct).reduceByKey(lambda a, b: a + b)
	auth_ct = auth_ct.map(lambda (a, b): (b, a)).sortByKey(ascending=False).map(lambda (a, b): (b, a))

	auth_net_edges = auth_ct.map(auth_ct_to_three_tpl)

	auth_cluster_mdl = PowerIterationClustering.train(auth_net_edges, 12, 25)

	result = sc.parallelize(sorted(auth_cluster_mdl.assignments().collect(), key=lambda x: x.id), numSlices=numParts)
	result = result.map(result_to_tpl)

	auth_net_clust = int_auth.join(result, numPartitions=numParts).map(lambda d: (d[1][0], d[1][1]))

	auth_net_edges_names = auth_net_edges.map(lambda d: (d[0], (d[1], d[2])))
	auth_net_edges_names = int_auth.join(auth_net_edges_names, numPartitions=numParts).map(lambda d: (d[1][1][0], (d[1][0], d[1][1][1])))
	auth_net_edges_names = int_auth.join(auth_net_edges_names, numPartitions=numParts).map(lambda d: (d[1][1][0], d[1][0], d[1][1][1]))

	auth_nodes_out = "s3://XX/auth_nodes2"
	auth_nodes_lines = auth_net_clust.map(auths_nodes_clusters_to_csv)
	auth_nodes_lines.coalesce(25).saveAsTextFile(auth_nodes_out)

	auth_edges_out = "s3://XX/auth_edges2"
	auth_edges_lines = auth_net_edges_names.map(auths_edges_to_csv)
	auth_edges_lines.coalesce(25).saveAsTextFile(auth_edges_out)

	sc.stop()





