# Students: A. Romriell, D. Wen, J. Pastor, J. Pollard
# MSAN 694 Project

from time import time
from glob import glob
import json
import re
from nltk.corpus import stopwords


ARXIV_DATA = "~/arxiv_data"
SUBJECTS = ["math", "cs", "physics", "stat", "q-bio", "q-fin"]


def create_abstract_vocab(data_dir, subjects):

    sw = stopwords.words("english")
    json_files = list()

    savepath = "{}/paper_words.txt".format(data_dir)

    for sub in subjects:

        json_path = data_dir + "/{}".format(sub)
        json_files += glob("{}/*.json".format(json_path))

    with open(savepath, "w") as outfile:

        for j in json_files:

            with open(j, "r") as fl:

                for line in fl:

                    md = json.loads(line)
                    paper_id = md["url"][0]
                    paper_id = paper_id.replace("http://", "")
                    abstract = md["abstract"][0]
                    words = abstract.strip()
                    words = re.sub(r'\\\\n', ' ', words)
                    words = re.sub(r'\$.*?\$', ' ', words)
                    words = re.sub(r'\W', ' ', words)
                    words = re.sub(r'\d', ' ', words)
                    words = words.lower()
                    words = words.split()
                    words = [w for w in words if len(w) > 3]
                    words = [w for w in words if w not in sw]
                    paper_words = "|".join(words)
                    outstr = "{}:{}\n".format(paper_id, paper_words)

                    outfile.write(outstr)


if __name__ == "__main__":

    start = time()

    create_abstract_vocab(ARXIV_DATA, SUBJECTS)

    print "This took {} seconds to run...".format(time() - start)

