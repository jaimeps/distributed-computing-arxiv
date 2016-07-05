# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project

import logging, json, os, sys, time
from datetime import datetime
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
import ConfigParser


def time_msg(t):

    diff = time.time() - t

    msg = ""

    if diff < 60:
        msg =  "This took {:.4f} seconds to run...".format(diff)

    elif diff >= 60 and diff < 3600:
        msg = "This took {:.4f} minutes to run...".format(diff / 60.0)

    else:
        msg = "This took {:.4f} hours to run...".format(diff / 3600.0)

    print msg
    logging.info(msg)


def arxiv_oai_scraper(subject, start, end, sleep_time=0):

    base_url = "http://export.arxiv.org/oai2"
    output = list()

    registry = MetadataRegistry()
    registry.registerReader('oai_dc', oai_dc_reader)
    client = Client(base_url, registry)
    client.updateGranularity()

    records = client.listRecords(metadataPrefix='oai_dc', set="{}".format(subject), from_=start, until=end)

    for _, md, _ in records:

        # print md.getField("title")
        # checks for the case in 2010 when there is no title for something
        if md is not None:

            txt_dict = {"title": md["title"],
                    "abstract": md["description"],
                    "date": md["date"],
                    "subject": md["subject"],
                    "url": md["identifier"],
                    "authors": md['creator']}

            output.append(txt_dict)

        time.sleep(sleep_time)

    return output


def check_dir(path):
    """
    check that path exists and is writable
    will raise an IOError exception if not writable
    :param path: path to check
    :return: True
    """
    try:
        os.stat(path)
    except:
        os.makedirs(path)

    if os.access(path, os.W_OK) == False:
        raise IOError("Error: not able to write to %s" % path)

    return True


def write_json(filepath, md_list):

    with open(filepath, "w") as outfl:

        for md in md_list:

            jsonify = json.dumps(md)
            out_str = jsonify + "\n"
            outfl.write(out_str)


if __name__ == "__main__":

    start = time.time()

    config = ConfigParser.ConfigParser()
    config.read('./user.cfg')

    subject = config.get('Scraper', 'subject', 0)
    path = config.get('Scraper', 'output_path', 0)

    # if no arguments passed, then read config file for dates
    #  otherwise the first two arguments are expected to be the start and stop date
    if len(sys.argv) == 1:

        start_date_str = config.get('Scraper', 'start_date', 0)
        stop_date_str = config.get('Scraper', 'stop_date', 0)

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        stop_date = datetime.strptime(stop_date_str, "%Y-%m-%d")
    else:
        start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        stop_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")

    log_path = config.get('Scraper', 'log_path', 0)
    check_dir(log_path)
    logging.basicConfig(filename="%s/arxiv_scraper.log" % log_path,level=logging.INFO,
                format='%(levelname)s %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %H:%M:%S')
    logging.info("************  Starting arxiv_scraper.py ****************")

    # check that output directory exists before doing (potentially) timely pull from arXiv
    check_dir(path)
    logging.info("Output path will be %s" % path)

    meta_data = arxiv_oai_scraper(subject=subject, start=start_date, end=stop_date)

    logfile_name = "log_%s_%s_%s.json" % (subject, start_date.date(), stop_date.date())
    filepath = "%s/%s" % (path, logfile_name)

    write_json(filepath, meta_data)

    logging.info("Wrote output to %s" % path)

    time_msg(start)

# end of arxiv_scraper.py
