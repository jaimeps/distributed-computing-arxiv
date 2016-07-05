# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project

import json
import codecs
import time
import urllib
from arxiv_scraper import time_msg
import glob


PATH = '~/arxiv_data/'

''' AUTHORS_TO_TXT FUNCTION ================================================='''
def authors_to_txt(file_input, file_output):
    """
    Extracts url and authors from json file and writes into a txt
    :param file_input: string with path to json file with data from ArXiv papers
    :param file_output: string with name of txt file
    :return: txt file with
    """
    f_json = open(file_input)
    f_text = codecs.open(file_output, 'a', encoding='utf-8')

    for line in f_json:
        js = json.loads(line)
        url = js['url'][0].replace('http://', '') + ':'
        authors = '|'.join(js['authors'])
        f_text.write(url + authors + '\n')

    f_text.close()
    f_json.close()


''' MAIN SCRIPT ============================================================='''
if __name__ == "__main__":

    # Areas and current iteration
    subjects = ['cs', 'math', 'physics','q-bio', 'q-fin', 'stat']

    for subject in subjects:
        # Obtain list of json files
        files = glob.glob(PATH + subject + '/*.json')

        # Name of output file
        file_output = PATH + 'authors.txt'

        # Load one file and convert to txt
        for file_input in files:
            start = time.time()
            print 'Converting file: ' + file_input
            authors_to_txt(file_input, file_output)
            time_msg(start)
