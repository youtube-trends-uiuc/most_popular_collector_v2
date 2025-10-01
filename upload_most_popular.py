import boto3
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

def upload_most_popular():
    s3 = boto3.resource('s3')
    creation_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    period = get_period()
    s3.Bucket('youtube-trends-uiuc-backup-v2').upload_file("./backup.json.bz2",
                                                      f"creation_date={creation_date}/period={period}/backup.json.bz2")
    s3.Bucket('youtube-trends-uiuc-v2').upload_file("./most_popular.orc",
                                                    f"most_popular/creation_date={creation_date}/period={period}/most_popular.orc")
    s3.Bucket('youtube-trends-uiuc-v2').upload_file("./categories.orc",
                                                    f"categories/creation_date={creation_date}/period={period}/categories.orc")
    s3.Bucket('youtube-trends-uiuc-v2').upload_file("./regions.orc",
                                                    f"regions/creation_date={creation_date}/period={period}/regions.orc")


# TODO: A system to tell us when there is an error.
if __name__ == '__main__':
    upload_most_popular()
