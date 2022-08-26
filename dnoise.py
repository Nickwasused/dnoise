#!/usr/bin/python3
# -*- coding: utf-8 -*-

from importlib import reload
from json import load
import logging
import datetime
import json
import os
import random
import sqlite3
import sys
import time
import urllib

import dns.resolver
import requests
import pandas

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


config = load_json("config.json")

# Set your pi-hole auth token - you can copy it from /etc/pihole/setupVars.conf
auth = config["auth"]

# Set the Fake Query Multiplier | Default: 1 = 10% Fake Query`s
multiplier = config["multiplier"]

# Set IP of the machine running this script. The script is optimized for running directly on the pi-hole server,
# or on another un-attended machine. "127.0.0.1" is valid only when running directly on the pi-hole.
client = config["client"]

# Set IP of your pi-hole instance. "127.0.0.1" is valid only when running directly on the pi-hole.
dns.resolver.nameservers = config["pihole_ip"]

# Logging to a file.
log_file = sys.stdout

# Set working directory for the script - the database with top 1M domains will be stored here.
working_directory = os.path.dirname(os.path.realpath(__file__))
zip_path = os.path.join(working_directory, "domains.zip")
database_path = os.path.join(working_directory, "domains.sqlite")

if auth == "":
    logging.warning("Please Set your auth token")
    sys.exit(1)


def download_domains():
    # Download the Cisco Umbrella list. More info: https://s3-us-west-1.amazonaws.com/umbrella-static/index.html
    try:
        logging.info("Downloading the domain list…")
        urllib.request.urlretrieve("https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip",
                                   filename=zip_path)
    except Exception as e:
        logging.error(e)
        logging.error("Can't download the domain list. Quitting.")
        sys.exit(1)

    # Create a SQLite database and import the domain list
    try:
        domains_db = sqlite3.connect(database_path)
        domains_db.execute("CREATE TABLE domains (ID INT PRIMARY KEY, Domain TEXT)")

        # Load the CSV into our database
        logging.info("Importing to sqlite…")
        df = pandas.read_csv(zip_path, compression='zip', names=["ID", "Domain"])
        df.to_sql("domains", domains_db, if_exists="append", index=False)

        domains_db.close()
        os.remove(zip_path)
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
        urllib.request.urlopen(check_url)
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

db = sqlite3.connect(database_path)

while True:
    # We want the fake queries to blend in with the organic traffic expected at each given time of the day,
    # so instead of having a static delay between individual queries, we'll sample the network activity over the past
    # 5 minutes and base the frequency on that. We want to add roughly 10% of additional activity in fake queries.
    time_until = int(time.mktime(datetime.datetime.now().timetuple()))
    time_from = time_until - 300

    # This will give us a list of all DNS queries that pi-hole handled in the past 5 minutes.
    pihole_tries = 0
    while True:
        if pihole_tries > 15:
            logging.error("Pihole seems to be down!")
            sys.exit(1)
        try:
            all_queries = requests.get(f"http://{config['pihole_ip']}/admin/api.php?getAllQueries&from={str(time_from)}&until={str(time_until)}&auth={auth}")
            break
        except Exception as e:
            pihole_tries += 1
            logging.error(e)
            logging.warning("API request failed. Retrying in 15 seconds.")
            time.sleep(15)

    parsed_all_queries = json.loads(all_queries.text)

    # When determining the rate of DNS queries on the network, we don't want our past fake queries to skew the
    # statistics, therefore we filter out queries made by this machine.
    genuine_queries = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                genuine_queries.append(a)
    except Exception as e:
        logging.error(e)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    # Protection in case the pi-hole logs are empty.
    if len(genuine_queries) == 0:
        genuine_queries.append("Let's not divide by 0")

    # We want the types of our fake queries (A/AAA/PTR/…) to proportionally match those of the real traffic.
    query_types = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                query_types.append(a[1])
    except Exception as e:
        logging.error(e)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    # Default to A request if pi-hole logs are empty
    if len(query_types) == 0:
        query_types.append("A")

    try:
        while True:
            # Pick a random domain from the top 1M list
            cursor = db.cursor()
            cursor.execute("SELECT domain FROM Domains WHERE ID = ?;", (str(random.randint(1, 1000000)), ))
            domain = cursor.fetchone()[0]
            cursor.close()

            # Try to resolve the domain - that's why we're here in the first place, isn't it…
            try:
                logging.info(f"resolving domain: {domain}")
                dns.resolver.resolve(domain, random.choice(query_types))
            except Exception as e:
                logging.warning(e)
                pass

            # We want to re-sample our "queries per last 5 min" rate every minute.
            if int(time.mktime(datetime.datetime.now().timetuple())) - time_until > 60:
                break

            # Since we want to add only about 10% of extra DNS queries, we multiply the wait time by 10, then add a
            # small random delay.
            time.sleep((300.0 / (len(genuine_queries)) * 10 / multiplier) + random.uniform(0, 2))
    except KeyboardInterrupt:
        try:
            break
        except SystemExit:
            break

logging.info("exiting")
db.close()
