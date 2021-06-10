import mysql.connector
import sqlite3
import requests

from datetime import datetime, timedelta
from login.login import login
from typing import Dict


def query_bids(credentials: Dict[str, str]):
    req_run = requests.get(
        'https://reporting.m6r.eu/v1/ssp',
        headers={
            'Authorization': 'Bearer {}'.format(credentials['token'])
        },
        params={
            'currency': 'eur',
            'dimensions': 'websiteName,date,dspPartnerName,assignedAgencyName,brandName',
            'format': 'json',
            'granularity': 'day',
            'metrics': 'adImpressions,sspPublisherPayout',
            'start-date': '{}'.format((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")),
            'end-date': '{}'.format(datetime.now().strftime("%Y-%m-%d")),
            'filters': 'adImpressions>0;sspPublisherPayout>0'
        },
        timeout=600
    )
    return req_run.json()['rows']


def get_domains_from_json(req_run):
    domains = {result[0] for result in req_run}
    return domains


def get_publisher_ids_dict(domains):
    connection = mysql.connector.connect(
        host=login.db_host,
        port="3306",
        user= login.db_user,
        password=login.db_password)

    cursor = connection.cursor(buffered=True)
    cursor.execute("""USE db00055768""")

    domains_dict = dict()
    for domain in domains:
        domain = domain.strip()
        query = """
                SELECT publisher.id
                FROM publisher
                JOIN website ON publisher.id = website.channel_id 
                WHERE website.name LIKE %s
                """
        cursor.execute(query, (domain[:domain.find('.') + 1] + '%',))
        domains_dict[domain] = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    return domains_dict


def parse_data(req_run, domains_dict):
    total = []
    for result in req_run:
        total.append([domains_dict[result[0].strip()], result[0].strip(), 'Ströer',
                      datetime.strptime(result[1], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d"),
                      result[2], result[3],result[4], result[5], result[6]])
    return total


def chunk_and_insert_into_db(total):
    bids_list = [total[i:i + 100000]for i in range(0, len(total), 100000)]
    connect = sqlite3.connect('bittersweet.db')
    cursor = connect.cursor()
    query = """
            INSERT INTO advertiser_bids(publisher_id, domain, network, date, dsp_partner_name, assigned_agency_name, 
                                        brand_name, ad_impressions, ssp_publisher_payout)
            VALUES(?,?,?,?,?,?,?,?,?)
            """
    for bid_list in bids_list:
        cursor.executemany(query, bid_list)
        connect.commit()
    cursor.close()
    connect.close()


if __name__ == "__main__":
    req_run = query_bids()
    domains = get_domains_from_json(req_run)
    domains_dict = get_publisher_ids_dict(domains)
    bids_list = parse_data(req_run, domains_dict)
    chunk_and_insert_into_db(bids_list)
