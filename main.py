#!/usr/bin/env python3
from dns.exception import DNSException
from dns.resolver import Resolver
from importlib import reload
from pathlib import Path
import urllib.request
import urllib.error
import configparser
import datetime
import zipfile
import logging
import random
import json
import time
import os
import sys
import csv

from database import Urls

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

reload(sys)

config_path = Path(__file__).parent.absolute().joinpath('config.ini')
config_reader = configparser.ConfigParser()
try:
    config_reader.read(config_path)
except configparser.ParsingError:
    logging.error("config file broken")
    sys.exit(1)

config = config_reader['DEFAULT']

# Set your pi-hole auth token - you can copy it from /etc/pihole/setupVars.conf
auth = config.get("auth", "")

# Logging to a file.
log_file = sys.stdout

# Set working directory for the script - the database with top 1M domains will be stored here.
working_directory = os.path.dirname(os.path.realpath(__file__))
zip_path = os.path.join(working_directory, "domains.zip")
csv_path = os.path.join(working_directory, "top-1m.csv")
database_path = os.path.join(working_directory, "domains.sqlite")


def download_domains() -> None:
    """Download the Cisco Umbrella list. More info: https://s3-us-west-1.amazonaws.com/umbrella-static/index.html"""

    try:
        with urllib.request.urlopen("https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip") as response:
            with open(zip_path, 'wb') as f:
                f.write(response.read())
    except urllib.error.HTTPError as error:
        logging.error(f"error while downloading domains: {error}")
        sys.exit(1)

    Urls().create_table()

    # Create a SQLite database and import the domain list
    try:
        # unzip the file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(working_directory)

        os.remove(zip_path)
        csv_file = open(csv_path, "r")
        domain_data = csv.reader(csv_file)

        Urls().mass_insert_urls(domain_data)

        csv_file.close()
        os.remove(csv_path)
    except Exception as e:
        logging.error(e)
        logging.error("Import failed. Quitting.")
        sys.exit(1)

    # Running this on 1st gen Raspberry Pi can take up to 10 minutes. Be patient.
    logging.info("Done downloading domains.")


def check_network() -> None:
    """Check if we have a network connection, because startup of Pi-hole takes a while after a restart.
    It is important that the network_check_url is not in the pihole blocklist!
    """
    network_try = 0
    retry_seconds = config.getint("network_retry_time", 10)
    network_check_url = config.get("network_check_url", "duckduckgo.com").replace("https://", "").replace("http://", "")
    check_resolver = Resolver()
    check_resolver.timeout = 1
    check_resolver.lifetime = 1

    while True:
        if network_try > config.getint("maximum_network_tries", 10):
            logging.error(f"Network is not up after {network_try} connection checks to url {network_check_url}. exiting.")
            sys.exit(1)
        try:
            _answer = check_resolver.resolve(network_check_url)
            logging.info("Got network connection.")
            break
        except DNSException as dns_error:
            logging.error(dns_error)
            logging.info(f"Network not up yet, retrying in {retry_seconds} seconds.")
            network_try += 1
            time.sleep(retry_seconds)


def get_genuine_queries(seconds_backwards: int = 300) -> [list[str], list[str]]:
    """We want the fake queries to blend in with the organic traffic expected at each given time of the day,
    so instead of having a static delay between individual queries, we'll sample the network activity over the past
    5 minutes (or a defined time range in seconds) and base the frequency on that. We want to add roughly 10% (or the defined threshold) of
    additional activity in fake queries.

    All queries by our ip are filtered, because otherwise we would increase the fake domains on every pass.

    The queries look like this:
        ['1678988772', 'A', 'bluelock.fandom.com', 'localhost', '2', '0', '3', '1034', 'N/A', '-1', '1.1.1.1', '']

    If there are no query types then it will default to an A record.
    """

    time_until = int(time.mktime(datetime.datetime.now().timetuple()))
    time_from = time_until - seconds_backwards

    pihole_tries = 0
    while True:
        if pihole_tries > 15:
            logging.error("Pihole seems to be down!")
            sys.exit(1)
        try:
            all_queries = urllib.request.urlopen(
                f"http://{config.get('pihole_ip')}/admin/api.php?getAllQueries&from={str(time_from)}"
                f"&until={str(time_until)}&auth={auth}&types=2,14,3,12,13").read()
            break
        except Exception as pihole_error:
            pihole_tries += 1
            logging.error(pihole_error)
            logging.warning("API request failed. Retrying in 15 seconds.")
            time.sleep(15)

    parsed_all_queries = json.loads(all_queries)
    tmp_genuine_queries = []
    try:
        for query in parsed_all_queries["data"]:
            if query[3] != client:
                tmp_genuine_queries.append(query)
    except IndexError as index_error:
        logging.error(index_error)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    if len(tmp_genuine_queries) == 0:
        tmp_genuine_queries.append("Let's not divide by 0")

    tmp_query_types = []
    try:
        for query in tmp_genuine_queries:
            tmp_query_types.append(query[1])
    except IndexError as index_error:
        logging.error(index_error)
        logging.error("Pi-hole API response in wrong format. Investigate.")
        sys.exit(1)

    if len(tmp_query_types) == 0:
        tmp_query_types.append("A")

    return tmp_query_types, tmp_genuine_queries


if __name__ == "__main__":
    if auth == "":
        logging.error("please set your auth token in the config.ini!")
        sys.exit(1)

    # Set IP of the machine running this script. The script is optimized for running directly on the pi-hole server,
    # or on another un-attended machine. "127.0.0.1" is valid only when running directly on the pi-hole.
    client = config.get("client", "127.0.0.1").replace("127.0.0.1", "localhost")

    # Set IP of your pi-hole instance. "127.0.0.1" is valid only when running directly on the pi-hole.
    pihole = Resolver(configure=False)
    nameservers = list()
    nameservers.append(config.get("pihole_ip", "127.0.0.1"))
    pihole.nameservers = nameservers
    pihole.timeout = 5

    check_network()

    # Download the top 1M domain list if we don't have it yet.
    exists = os.path.isfile(database_path)
    if not exists:
        download_domains()

    # 7 days = 604400
    if (time.time() - os.path.getctime(database_path)) > 604400 and config.getboolean("keep_database_updated", True):
        logging.warning("the domain data is old. downloading new data")
        os.remove(database_path)
        download_domains()

    while True:
        seconds = 60
        query_types, genuine_queries = get_genuine_queries(seconds)

        try:
            # We got a time window of "seconds": first we calculate the total amount of query's that we need to send in
            # that time window
            query_amount = round(len(genuine_queries) / 100 * config.getint("percentage_fake_data", 10))
            if query_amount == 0:
                query_amount = 1
            # After that we need to calculate the timeout between each query to reach our target amount
            timeout = seconds / query_amount
            # Now we send our first query and wait for the timeout!

            queries = Urls().get_random_domains(query_amount)
            current_query_count = 0

            while True:
                # select a random domain
                domain = queries[current_query_count]
                query_type = random.choice(query_types)

                # Try to resolve the domain - that's why we're here in the first place, isn't it…
                try:
                    logging.info(f"resolving domain: {domain} with type {query_type}")
                    # resolve the query with a random query type that we got from the genuine queries
                    # e.g. resolve duckduckgo.com with the type AAAA or PTR
                    pihole.resolve(domain, query_type)
                except Exception as e:
                    logging.warning(e)
                    pass

                current_query_count = current_query_count + 1
                # wait
                time.sleep(timeout)
                # do the next request or exit the loop
                if current_query_count == query_amount:
                    break

            logging.info(f"sent {query_amount} query's in {seconds} seconds when original were {len(genuine_queries)}")
            # Our defined time in seconds should have passed by now and we re-sample our "queries per last time window"
        except KeyboardInterrupt:
            try:
                break
            except SystemExit:
                break

    logging.info("exiting")
