import codecs
import glob
import json
import re
import sys
import ConfigParser
import file_mgmt

# format will be
# arxiv.org/...|subj|dt|title

if __name__ == '__main__':

    config = ConfigParser.ConfigParser()
    config.read('./user.cfg')

    input_path = config.get('General', 'user_path', 0)
    output_path = config.get('General', 'output_path', 0)

    subject_list = ['cs', 'math', 'physics', 'q-bio', 'q-fin', 'stat']

    file_mgmt.check_dir(output_path)

    for subject in subject_list:

        file_list = glob.glob("%s/%s/*.json" % (input_path, subject))

        file_no = 0

        for fn in file_list:

            file_no += 1;

            with codecs.open("%s/%s-%02d.txt" % (output_path, subject, file_no), "w", encoding='UTF-8-sig') as outfl:

                print fn

                data = []

                with codecs.open(fn, encoding='UTF-8-sig') as f:

                    for line in f:
                        data.append(json.loads(line))

                for line in data:

                    # remove "http://" from front of title
                    url_raw = line['url'][0]
                    url = re.sub('^http://', '', url_raw)

                    date = line['date'][0]

                    title = re.sub('\n', '', line['title'][0])

                    outfl.write("%s|%s|%s|%s\n" % (url, subject, date, title))




