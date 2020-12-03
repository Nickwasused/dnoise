#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import json
import os
import random
import sqlite3
import sys
import time
import urllib
from importlib import reload

import dns.resolver
import pandas
import requests

reload(sys)

#########################################################################################
#				BEGINNING OF CONFIG SECTION				#

# Set working directory for the script - the database with top 1M domains will be stored here.
working_directory = "/home/pi/noise/"

# Set your pi-hole auth token - you can copy it from /etc/pihole/setupVars.conf
auth = ""

# Set IP of the machine running this script. The script is optimized for running directly on the pi-hole server,
# or on another un-attended machine. "127.0.0.1" is valid only when running directly on the pi-hole.
client = "127.0.0.1"

# Set IP of your pi-hole instance. "127.0.0.1" is valid only when running directly on the pi-hole.
dns.resolver.nameservers = "127.0.0.1"

# Logging to a file. For easier debugging uncomment the second row.
log_file = sys.stdout


#				  END OF CONFIG SECTION  				#
#########################################################################################

def download_domains():
    # Download the Cisco Umbrella list. More info: https://s3-us-west-1.amazonaws.com/umbrella-static/index.html
    try:
        print("Downloading the domain list…")
        urllib.request.urlretrieve("http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip",
                           filename=working_directory + "domains.zip")
    except Exception as e:
        print(e)
        print("Can't download the domain list. Quitting.")
        exit()

    # Create a SQLite database and import the domain list
    try:
        db = sqlite3.connect(working_directory + "domains.sqlite")
        db.execute("CREATE TABLE Domains (ID INT PRIMARY KEY, Domain TEXT)")

        # Load the CSV into our database
        print("Importing to sqlite…")
        df = pandas.read_csv(working_directory + "domains.zip", compression='zip', names=["ID", "Domain"])
        df.to_sql("Domains", db, if_exists="append", index=False)

        db.close()

        os.remove(working_directory + "domains.zip")
    except:
        print("Import failed. Quitting.")
        exit()

    # Running this on 1st gen Raspberry Pi can take up to 10 minutes. Be patient.
    print("Done.")


# A simple loop that makes sure we have an Internet connection - it can take a while for pi-hole to get up and running after a reboot.
while True:
    try:
        urllib.request.urlopen("https://duckduckgo.com")
        print("Got network connection.")
        break
    except Exception as e:
        print(e)
        print("Network not up yet, retrying in 10 seconds.")
        time.sleep(10)

# Download the top 1M domain list if we don't have it yet.
exists = os.path.isfile(working_directory + "domains.sqlite")
if not exists:
    download_domains()

db = sqlite3.connect(working_directory + "domains.sqlite")

while True:
    # We want the fake queries to blend in with the organic traffic expected at each given time of the day, so instead of having a static delay between individual queries,
    # we'll sample the network activity over the past 5 minutes and base the frequency on that. We want to add roughly 10% of additional activity in fake queries.
    time_until = int(time.mktime(datetime.datetime.now().timetuple()))
    time_from = time_until - 300

    # This will give us a list of all DNS queries that pi-hole handled in the past 5 minutes.
    while True:
        try:
            all_queries = requests.get(
                "http://pi.hole/admin/api.php?getAllQueries&from=" + str(time_from) + "&until=" + str(
                    time_until) + "&auth=" + auth)
            break
        except:
            print(" API request failed. Retrying in 15 seconds.")
            time.sleep(15)

    parsed_all_queries = json.loads(all_queries.text)

    # When determining the rate of DNS queries on the network, we don't want our past fake queries to skew the statistics, therefore we filter out queries made by this machine.
    genuine_queries = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                genuine_queries.append(a)
    except:
        print(" Pi-hole API response in wrong format. Investigate.")
        exit()

    # Protection in case the pi-hole logs are empty.
    if len(genuine_queries) == 0:
        genuine_queries.append("Let's not devide by 0")

    # We want the types of our fake queries (A/AAA/PTR/…) to proportionally match those of the real traffic.
    query_types = []
    try:
        for a in parsed_all_queries["data"]:
            if a[3] != client.replace("127.0.0.1", "localhost"):
                query_types.append(a[1])
    except:
        print("Pi-hole API response in wrong format. Investigate.")
        exit()

    # Default to A request if pi-hole logs are empty
    if len(query_types) == 0:
        query_types.append("A")

    while True:
        # Pick a random domain from the top 1M list
        rand = str(random.randint(1, 1000000))
        cursor = db.cursor()
        cursor.execute("SELECT Domain FROM Domains WHERE ID=" + rand)
        domain = cursor.fetchone()[0]

        # Try to resolve the domain - that's why we're here in the first place, isn't it…
        try:
            dns.resolver.resolve(domain, random.choice(query_types))
        except:
            pass

        # We want to re-sample our "queries per last 5 min" rate every minute.
        if int(time.mktime(datetime.datetime.now().timetuple())) - time_until > 60:
            break

        # Since we want to add only about 10% of extra DNS queries, we multiply the wait time by 10, then add a small random delay.
        time.sleep((300.0 / (len(genuine_queries)) * 10) + random.uniform(0, 2))

db.close()
