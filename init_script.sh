#!/bin/bash
sudo timedatectl set-timezone UTC
sudo apt-get install -y python3-pip python3-venv default-jre
cd /home/ubuntu
wget https://repo1.maven.org/maven2/org/apache/orc/orc-tools/2.2.0/orc-tools-2.2.0-uber.jar
wget https://raw.githubusercontent.com/youtube-trends-uiuc/most_popular_collector/refs/heads/main/collect_most_popular.py
wget https://raw.githubusercontent.com/youtube-trends-uiuc/most_popular_collector/refs/heads/main/requirements.txt
wget https://raw.githubusercontent.com/youtube-trends-uiuc/most_popular_collector/refs/heads/main/upload_most_popular.py
python3 -m venv ./venv
source ./venv/bin/activate
pip3 install --trusted-host pypi.python.org -r ./requirements.txt
# where to find how to format the timestamp in orc-tools: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
python3 ./collect_most_popular.py && \
java -jar ./orc-tools-2.2.0-uber.jar convert most_popular.json -s 'struct<kind:string,etag:string,id:string,snippet:struct<publishedAt:timestamp,title:string,description:string,channelId:string,channelTitle:string,categoryId:string,tags:array<string>,liveBroadcastContent:string,defaultLanguage:string,defaultAudioLanguage:string,localized:struct<title:string,description:string>,thumbnails:struct<default:struct<url:string,width:int,height:int>,medium:struct<url:string,width:int,height:int>,high:struct<url:string,width:int,height:int>,standard:struct<url:string,width:int,height:int>,maxres:struct<url:string,width:int,height:int>>>,statistics:struct<viewCount:bigint,likeCount:bigint,dislikeCount:bigint,favoriteCount:bigint,commentCount:bigint>,metadata:struct<region_code:string,category_id:string,retrieved_at:timestamp,rank:int>>' -o most_popular.orc -t "yyyy-MM-dd HH:mm:ss.nX" 2>&1 | tee ./orc_most_popular_output.log && \
java -jar ./orc-tools-2.2.0-uber.jar convert regions.json -s 'struct<id:string,snippet:struct<name:string>,metadata:struct<retrieved_at:timestamp>>' -o regions.orc -t "yyyy-MM-dd HH:mm:ss.nX" 2>&1 | tee ./orc_regions_output.log && \
java -jar ./orc-tools-2.2.0-uber.jar convert categories.json -s 'struct<id:string,snippet:struct<title:string,assignable:boolean>,metadata:struct<region_code:string,retrieved_at:timestamp>>' -o categories.orc -t "yyyy-MM-dd HH:mm:ss.nX" 2>&1 | tee ./orc_categories_output.log && \
bzip2 -z --best ./backup.json 2>&1 | tee ./bzip2_output.log && \
python3 ./upload_most_popular.py
# && \
# sudo shutdown -h now