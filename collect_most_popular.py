import ssl
import smtplib
from email.message import EmailMessage
import requests
import json
import logging
import time
import googleapiclient.discovery
from googleapiclient.errors import HttpError, UnknownApiNameOrVersion
import datetime
from socket import error as socket_error
import errno
import boto3


WAIT_WHEN_SERVICE_UNAVAILABLE = 30
WAIT_WHEN_CONNECTION_RESET_BY_PEER = 60
WAIT_WHEN_UNKNOWN_ERROR = 180


def get_youtube_client(developer_key):
    unknown_error = 0
    youtube = None
    no_response = True
    while no_response:
        try:
            youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                      version="v3",
                                                      developerKey=developer_key,
                                                      cache_discovery=False)
            no_response = False
        except UnknownApiNameOrVersion as e:
            page = requests.get("https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
            service = json.loads(page.text)
            youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                    developerKey=developer_key)
        except Exception as e:
            logging.error(e)
            unknown_error += 1
            if unknown_error <= 3:
                time.sleep(WAIT_WHEN_UNKNOWN_ERROR)
            else:
                raise

    return youtube


def get_response_from_youtube(response_type, request_params, youtube=None, developer_key=None):
    if developer_key is None:
        developer_key = read_developer_key()
    if youtube is None:
        youtube = get_youtube_client(developer_key=developer_key)
    no_response = True
    unknown_error = 0
    connection_reset_by_peer = 0
    service_unavailable = 0
    response = None
    while no_response:
        try:
            if response_type == "regions":
                request = youtube.i18nRegions().list(**request_params)
            elif response_type == "videos":
                request = youtube.videos().list(**request_params)
            elif response_type == "categories":
                request = youtube.videoCategories().list(**request_params)
            else:
                raise Exception("Unknown response type")
            response = request.execute()
            no_response = False
        except socket_error as e:
            if e.errno != errno.ECONNRESET:
                logging.info("Other socket error!")
                raise
            else:
                connection_reset_by_peer = connection_reset_by_peer + 1
                logging.info("Connection reset by peer! {}".format(connection_reset_by_peer))
                if connection_reset_by_peer <= 10:
                    time.sleep(WAIT_WHEN_CONNECTION_RESET_BY_PEER)
                    youtube = get_youtube_client(developer_key=developer_key)
                else:
                    raise
        except HttpError as e:
            if "403" in str(e):
                logging.info(f"403 - Quota Exceeded. Credential: {developer_key}")
                emergency_developer_key = read_developer_key(emergency=True)
                if emergency_developer_key != developer_key:
                    developer_key = emergency_developer_key
                    youtube = get_youtube_client(developer_key=developer_key)
                else:
                    raise
            elif "503" in str(e):
                logging.info("503 - Service unavailable")
                service_unavailable = service_unavailable + 1
                if service_unavailable <= 10:
                    time.sleep(WAIT_WHEN_SERVICE_UNAVAILABLE)
                else:
                    raise
            elif "429" in str(e):
                logging.info("429 - Too Many Requests")
                service_unavailable = service_unavailable + 1
                if service_unavailable <= 10:
                    time.sleep(WAIT_WHEN_SERVICE_UNAVAILABLE)
                else:
                    raise
            else:
                logging.info("Unknown HttpError")
                raise
        except Exception as e:
            logging.error(e)
            unknown_error += 1
            if unknown_error <= 3:
                time.sleep(WAIT_WHEN_UNKNOWN_ERROR)
            else:
                raise
    return response, youtube, developer_key


def get_period():
    period = int(datetime.datetime.now(datetime.UTC).strftime("%H"))
    if period < 6:
        return '00'
    elif period < 12:
        return '06'
    elif period < 18:
        return '12'
    else:
        return '18'

def add_dict_to_file(file_handler, record, retrieved_at, request_params=None):
    if 'metadata' not in record:
        record['metadata'] = dict()
    record['metadata']['retrieved_at'] = retrieved_at
    if request_params is not None:
        record['metadata']['request_params'] = request_params
    file_handler.write(json.dumps(record) + '\n')


def send_gmail(subject, message):
    s3 = boto3.resource('s3')
    content_object = s3.Object('youtube-trends-uiuc-admin', 'smtp.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    smtp = json.loads(file_content)

    msg = EmailMessage()
    msg["From"] = smtp['sender']
    msg["To"] = smtp['receiver']
    msg["Subject"] = subject
    msg.set_content(message)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(smtp['sender'], smtp['app_password'])
        smtp.send_message(msg)


def read_developer_key(emergency=False):
    period = get_period()
    s3 = boto3.resource('s3')
    if emergency:
        content_object = s3.Object('youtube-trends-uiuc-admin', 'credentials_emergency.json')
    else:
        content_object = s3.Object('youtube-trends-uiuc-admin', 'credentials.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    credentials = json.loads(file_content)
    return credentials[period]

def get_unspecified_category(region_code):
    unspecified_category = {
        'kind': 'youtube#videoCategory',
        'etag': 'NA',
        'id': '0',
        'snippet': {
            'title': 'Unspecified',
            'assignable': True,
            'channelId': 'NA'
        },
        'metadata': {
            'region_code': region_code
        }
    }
    return unspecified_category

def collect_most_popular():
    with open('./backup.json', 'w') as backup_json:
        retrieved_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.%f") + "000Z"
        request_params = {'part': 'snippet'}
        regions, youtube, developer_key = get_response_from_youtube(response_type="regions",
                                                                    request_params=request_params)
        add_dict_to_file(backup_json, regions, retrieved_at, request_params=request_params)

        with open('./regions.json', 'w') as regions_json:
            region_codes = []
            for region in regions.get('items', []):
                region_codes.append(region['id'])
                add_dict_to_file(regions_json, region, retrieved_at)
            region_codes.sort()

        with open('./categories.json', 'w') as categories_json:
            with open('./most_popular.json', 'w') as most_popular_json:
                for region_code in region_codes:
                    retrieved_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.%f") + "000Z"
                    request_params = {
                        'part': 'snippet',
                        'regionCode': region_code
                    }
                    categories, youtube, developer_key = get_response_from_youtube(response_type="categories",
                                                                                   request_params=request_params,
                                                                                   youtube=youtube,
                                                                                   developer_key=developer_key)
                    add_dict_to_file(backup_json, categories, retrieved_at, request_params=request_params)

                    # The next two lines artificially add a "zero" category.
                    # It corresponds to a call to videos.list(most_popular) when no category is specified.
                    category_ids = ['0', ]
                    add_dict_to_file(categories_json, get_unspecified_category(region_code), retrieved_at)
                    # TODO: create a script to fix the first five periods when I didn't add the 'Unspecified' category.

                    for category in categories.get('items', []):
                        if category['snippet']['assignable']:
                            category_ids.append(category['id'])
                        category['metadata'] = dict()
                        category['metadata']['region_code'] = region_code
                        add_dict_to_file(categories_json, category, retrieved_at)
                    category_ids.sort()

                    for category_id in category_ids:
                        rank = 1
                        next_page_token = None
                        more_pages = True

                        # Loop through all pages for this region
                        while more_pages:
                            retrieved_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.%f") + "000Z"
                            request_params = {
                                'part': 'snippet,statistics',
                                'chart': 'mostPopular',
                                'regionCode': region_code,
                                'maxResults': 50,
                                'videoCategoryId': category_id
                            }
                            if next_page_token:
                                request_params['pageToken'] = next_page_token
                            try:
                                videos, youtube, developer_key = get_response_from_youtube(response_type="videos",
                                                                                           request_params=request_params,
                                                                                           youtube=youtube,
                                                                                           developer_key=developer_key)
                                add_dict_to_file(backup_json, videos, retrieved_at, request_params=request_params)
                            except HttpError as e:
                                if "Requested entity was not found." in str(e):
                                    logging.info("404 - Requested entity was not found.")
                                    videos = {}
                                else:
                                    raise

                            # Process items in the current page
                            for video in videos.get('items', []):
                                video['snippet']['publishedAt'] = video['snippet']['publishedAt'].replace('Z', '.000000000Z').replace('T', ' ')
                                video['metadata'] = dict()
                                video['metadata']['region_code'] = region_code
                                video['metadata']['category_id'] = category_id
                                video['metadata']['rank'] = rank
                                rank = rank + 1
                                add_dict_to_file(most_popular_json, video, retrieved_at)

                            # Check if there's a next page
                            next_page_token = videos.get('nextPageToken')
                            if not next_page_token:
                                more_pages = False


if __name__ == '__main__':
    try:
        collect_most_popular()
    except Exception as e:
        send_gmail('Error! Please check AWS', f'Hi, my friend!\n\nThe script collect_most_popular.py has just failed with this error:\n\n{str(e)}\n\nYou need to visit AWS EC2 to see what happened.\n\nAll the best,\nAdmin.')
        raise e
