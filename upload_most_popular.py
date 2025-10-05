import ssl
import smtplib
from email.message import EmailMessage
import bz2
import shutil
import boto3
import json
import os
import subprocess
import argparse
import datetime

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

STRUCT_MOST_POPULAR = 'struct<kind:string,etag:string,id:string,snippet:struct<publishedAt:timestamp,title:string,description:string,channelId:string,channelTitle:string,categoryId:string,tags:array<string>,liveBroadcastContent:string,defaultLanguage:string,defaultAudioLanguage:string,localized:struct<title:string,description:string>,thumbnails:struct<default:struct<url:string,width:int,height:int>,medium:struct<url:string,width:int,height:int>,high:struct<url:string,width:int,height:int>,standard:struct<url:string,width:int,height:int>,maxres:struct<url:string,width:int,height:int>>>,statistics:struct<viewCount:bigint,likeCount:bigint,dislikeCount:bigint,favoriteCount:bigint,commentCount:bigint>,metadata:struct<region_code:string,category_id:string,retrieved_at:timestamp,rank:int>>'
STRUCT_REGION = 'struct<id:string,snippet:struct<name:string>,metadata:struct<retrieved_at:timestamp>>'
STRUCT_CATEGORIES = 'struct<id:string,snippet:struct<title:string,assignable:boolean>,metadata:struct<region_code:string,retrieved_at:timestamp>>'

def convert_to_orc(file, struct, min_size = 0):
    # Build the command as a list to avoid shell-quoting issues on any OS
    cmd = [
        "java", "-jar", "./orc-tools-2.2.0-uber.jar",
        "convert", f"./{file}.json",
        "-s", struct,
        "-o", f"./{file}.orc",
        "-t", "yyyy-MM-dd HH:mm:ss.nX",
        "--overwrite"
    ]

    file_created = False
    small_file = False
    attempt = 0

    while (not file_created or small_file) and attempt < 3:
        # Stream stdout and stderr to the log file in real time
        with open(f"./orc_{file}_output.log", "a", encoding="utf-8") as log:
            log.write("Running command:\n" + " ".join(cmd) + "\n\n")
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            return_code = proc.wait()
            log.write(f"\nProcess exited with code {return_code}\n")

        if return_code == 0:
            file_created = True
            file_size = os.path.getsize(f"./{file}.orc")
            if file_size <= min_size:
                small_file = True
                attempt += 1
            else:
                small_file = False
        else:
            attempt += 1

    return file_created, small_file


def compress_bzip2(filename, min_size = 0):
    compressed_filename = filename + '.bz2'
    file_created = False
    small_file = False
    attempt = 0
    while (not file_created or small_file) and attempt < 3:
        with open(filename, 'rb') as source_file, bz2.BZ2File(compressed_filename, 'wb', compresslevel=9) as compressed_file:
            shutil.copyfileobj(source_file, compressed_file)
        file_created = True
        file_size = os.path.getsize(compressed_filename)
        if file_size <= min_size:
            small_file = True
            attempt += 1
        else:
            small_file = False
    return compressed_filename, small_file


def upload_most_popular(creation_date, period):
    s3 = boto3.resource('s3')
    print('Compressing backup.json.')
    compressed_backup, small_backup = compress_bzip2('./backup.json', min_size=30 * 1024 * 1024)
    print('Uploading backup.json')
    s3.Bucket('youtube-trends-uiuc-backup-v2').upload_file(compressed_backup,
                                                           f"creation_date={creation_date}/period={period}/backup.json.bz2")
    print('Converting regions.json')
    regions_created, small_regions = convert_to_orc('regions', STRUCT_REGION)
    if regions_created:
        print('Uploading regions.orc')
        s3.Bucket('youtube-trends-uiuc-v2').upload_file("./regions.orc",
                                                        f"regions/creation_date={creation_date}/period={period}/regions.orc")
    print('Converting categories.json')
    categories_created, small_categories = convert_to_orc('categories', STRUCT_CATEGORIES)
    if categories_created:
        print('Uploading categories.orc')
        s3.Bucket('youtube-trends-uiuc-v2').upload_file("./categories.orc",
                                                        f"categories/creation_date={creation_date}/period={period}/categories.orc")
    print('Converting most_popular.json')
    most_popular_created, small_most_popular = convert_to_orc('most_popular',
                                                              STRUCT_MOST_POPULAR,
                                                              min_size=30 * 1024 * 1024) # 30 Mb... the normal size is ~60 Mb.
    if most_popular_created:
        print('Uploading most_popular.orc')
        s3.Bucket('youtube-trends-uiuc-v2').upload_file("./most_popular.orc",
                                                        f"most_popular/creation_date={creation_date}/period={period}/most_popular.orc")

    if not most_popular_created or small_most_popular:
        raise Exception("Error generating most_popular.orc.")
    elif not categories_created or small_categories:
        raise Exception("Error generating categories.orc.")
    elif not regions_created or small_regions:
        raise Exception("Error generating regions.orc.")
    elif small_backup:
        raise Exception("Backup file is too small.")


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

    # Gmail SMTP over SSL (port 465). Alternatively, use STARTTLS on port 587.
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(smtp['sender'], smtp['app_password'])
        smtp.send_message(msg)


def main():
    try:
        parser = argparse.ArgumentParser(description="Upload most popular items.")
        parser.add_argument("creation_date", nargs="?", help="YYYY-MM-DD")
        parser.add_argument("period", nargs="?", help="00, 06, 12, 18")
        args = parser.parse_args()

        creation_date = args.creation_date or datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        period = args.period or get_period()

        upload_most_popular(creation_date, period)
    except Exception as e:
        send_gmail('Error! Please check AWS', 'Hi, my friend!\n\nThe script upload_most_popular.py has just failed. You need to visit AWS EC2 to see what happened.\n\nAll the best,\nAdmin.')
        raise e

if __name__ == '__main__':
    main()
