#!/usr/bin/python3
# -*- coding: utf-8 -*-

from urllib.request import urlretrieve, urlopen
from dns.resolver import Resolver
from importlib import reload
from json import load
import datetime
import zipfile
import logging
import sqlite3
import random
import json
import time
import os
import sys
import csv

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

reload(sys)


def load_json(file):
    try:
        f = open(file, encoding="utf8")
    except FileNotFoundError:
        logging.warning("The config file is missing: config.json")
        sys.exit(1)
    return load(f)


try:
    config = load_json("config.json")
except Exception as e:
    logging.error(e)
    logging.error("your config file is broken!")
    sys.exit(1)

# Set your pi-hole auth token - you can copy it from /etc/pihole/setupVars.conf
auth = config["auth"]

# Set IP of the machine running this script. The script is optimized for running directly on the pi-hole server,
# or on another un-attended machine. "127.0.0.1" is valid only when running directly on the pi-hole.
client = config["client"]

# Set IP of your pi-hole instance. "127.0.0.1" is valid only when running directly on the pi-hole.
pihole = Resolver(configure=False)
nameservers = list()
nameservers.append(config["pihole_ip"])
pihole.nameservers = nameservers
pihole.timeout = 5

# Logging to a file.
log_file = sys.stdout

# Set working directory for the script - the database with top 1M domains will be stored here.
working_directory = os.path.dirname(os.path.realpath(__file__))
zip_path = os.path.join(working_directory, "domains.zip")
database_path = os.path.join(working_directory, "domains.sqlite")
csv_path = os.path.join(working_directory, "top-1m.csv")

if auth == "":
    logging.warning("Please Set your auth token")
    sys.exit(1)


def chunks(data, rows=10000):
    for i in range(0, len(data), rows):
        yield data[i:i+rows]


def download_domains():
    # Download the Cisco Umbrella list. More info: https://s3-us-west-1.amazonaws.com/umbrella-static/index.html
    try:
        logging.info("Downloading the domain list…")
        urlretrieve("https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip", filename=zip_path)
    except Exception as e:
        logging.error(e)
        logging.error("Can't download the domain list. Quitting.")
        sys.exit(1)

    # Create a SQLite database and import the domain list
    try:
        domains_db = sqlite3.connect(database_path)
        domains_db.execute("CREATE TABLE domains (url TEXT)")

        # unzip the file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(working_directory)

        os.remove(zip_path)
        csv_file = open(csv_path, "r")
        domain_data = csv.reader(csv_file)
        chunk_data = chunks(list(domain_data))

        cursor = domains_db.cursor()
        for chunk in chunk_data:
            cursor.execute('BEGIN TRANSACTION')
            for ID, Domain in chunk:
                cursor.execute('INSERT INTO domains (url) VALUES (?)', (Domain, ))
            cursor.execute('COMMIT')

        cursor.close()
        domains_db.close()
        csv_file.close()
        os.remove(csv_path)
    except Exception as e:
        logging.error(e)
        logging.error("Import failed. Quitting.")
        sys.exit(1)

    # Running this on 1st gen Raspberry Pi can take up to 10 minutes. Be patient.
    logging.info("Done downloading domains.")


# A simple loop that makes sure we have an Internet connection - it can take a while for pi-hole to get up and
# running after a reboot.
network_try = 0
retry_seconds = config["network_retry_time"]
check_url = config["network_check_url"]

if "http" not in check_url:
    logging.warning("There is no protocol specified in your network_check_url. Using https!")
    check_url = f"https://{check_url}"
    logging.info(f"New network_check_url: {check_url}")

while True:
    if network_try > config["maximum_network_tries"]:
        logging.error(f"Network is not up after {network_try} connection checks to url {check_url}. exiting.")
        sys.exit(1)
    try:
        urlopen(check_url)
        logging.info("Got network connection.")
        break
    except Exception as e:
        logging.error(e)
        logging.info(f"Network not up yet, retrying in {retry_seconds} seconds.")
        network_try += 1
        time.sleep(retry_seconds)

# Download the top 1M domain list if we don't have it yet.
exists = os.path.isfile(database_path)
if not exists:
    download_domains()

# 7 days = 604400
if (time.time() - os.path.getctime(database_path)) > 604400 and config["keep_database_updated"]:
    logging.warning("the domain data is old. downloading new data")
    os.remove(database_path)
    download_domains()

db = sqlite3.connect(database_path)


def get_random_domain():
    cursor = db.cursor()
    # https://web.archive.org/web/20200628215538/http://www.bernzilla.com/2008/05/13/selecting-a-random-row-from-an-sqlite-table/
    cursor.execute("SELECT url FROM Domains ORDER BY RANDOM() LIMIT 1;")
    rnd_domain = cursor.fetchone()[0]
    cursor.close()
    return rnd_domain


def get_genuine_querys(seconds=300):
    # We want the fake queries to blend in with the organic traffic expected at each given time of the day,
    # so instead of having a static delay between individual queries, we'll sample the network activity over the past
    # 5 minutes and base the frequency on that. We want to add roughly 10% of additional activity in fake queries.
    time_until = int(time.mktime(datetime.datetime.now().timetuple()))
    time_from = time_until - seconds

    # This will give us a list of all DNS queries that pi-hole handled in the past 5 minutes.
    pihole_tries = 0
    while True:
        if pihole_tries > 15:
            logging.error("Pihole seems to be down!")
            sys.exit(1)
        try:
            all_queries = urlopen(
                f"http://{config['pihole_ip']}/admin/api.php?getAllQueries&from={str(time_from)}&until={str(time_until)}&auth={auth}&types=2,14,3,12,13").read()
            break
        except Exception as e:
            pihole_tries += 1
            logging.error(e)
            logging.warning("API request failed. Retrying in 15 seconds.")
            time.sleep(15)

    parsed_all_queries = json.loads(all_queries)

    # When determining the rate of DNS queries on the network, we don't want our past fake queries to skew the
    # statistics, therefore we filter out queries made by this machine.
    tmp_genuine_queries = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                tmp_genuine_queries.append(a)
    except Exception as e:
        logging.error(e)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    # Protection in case the pi-hole logs are empty.
    if len(tmp_genuine_queries) == 0:
        tmp_genuine_queries.append("Let's not divide by 0")

    # We want the types of our fake queries (A/AAA/PTR/…) to proportionally match those of the real traffic.
    tmp_query_types = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                tmp_query_types.append(a[1])
    except Exception as e:
        logging.error(e)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    # Default to A request if pi-hole logs are empty
    if len(tmp_query_types) == 0:
        tmp_query_types.append("A")

    return tmp_query_types, tmp_genuine_queries


while True:
    seconds = 60
    query_types, genuine_queries = get_genuine_querys(seconds)

    try:
        # We got a time window of "seconds": first we calculate the total amount of query's that we need to send in
        # that time window
        query_amount = round(len(genuine_queries) / 100 * config["percentage_fake_data"])
        if query_amount == 0:
            query_amount = 1
        # After that we need to calculate the timeout between each query to reach our target amount
        timeout = seconds / query_amount
        # Now we send our first query and wait for the timeout!

        current_query_count = 0
        while True:
            # Pick a random domain from the top 1M list
            try:
                domain = get_random_domain()
            except Exception as e:
                logging.error(e)
                logging.error("Please restart the script. The domains.sqlite file got removed!")
                os.remove(database_path)
                sys.exit(1)

            # Try to resolve the domain - that's why we're here in the first place, isn't it…
            try:
                logging.info(f"resolving domain: {domain}")
                pihole.resolve(domain, random.choice(query_types))
            except Exception as e:
                logging.warning(e)
                pass

            # wait
            current_query_count = current_query_count + 1
            time.sleep(timeout)
            # do the next request or exit the loop
            if current_query_count == query_amount:
                break

        logging.info(f"sent {query_amount} query's in {seconds} seconds when original were {len(genuine_queries)}")
        # 5 minutes should have passed by now and we re-sample our "queries per last 5 min"
    except KeyboardInterrupt:
        try:
            break
        except SystemExit:
            break

logging.info("exiting")
db.close()
