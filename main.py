import googleapiclient.discovery
import pymongo
from pymongo import MongoClient
# import plotly.express as px
import mysql.connector
import pandas as pd
import streamlit as st

youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey='AIzaSyBoEwsXupI9t-6R5yHX10fmt8wd0Kuj_Oc')

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

#Establish the mysql connection
connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root123',
        autocommit=True
)




mycursor = connection.cursor(buffered=True)

yt_db = myclient['yt_database']
yt_col = yt_db['yt_collection']


# In[8]:


# Function to retrieve channel data
def get_channel_data(channel_id):
    # Fetch channel details
    ch_data = []
    channel_response = youtube.channels().list(part='snippet, contentDetails, statistics', id=channel_id).execute()

    # Extract relevant information
    for i in range(len(channel_response['items'])):
        channel_data = {
            'channel_Id': channel_id,
            'channel_name': channel_response['items'][i]['snippet']['title'],
            'subscribers': int(channel_response['items'][i]['statistics']['subscriberCount']),
            'Playlist_id': channel_response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            'total_videos': int(channel_response['items'][i]['statistics']['videoCount']),
            'Views': int(channel_response['items'][i]['statistics']['viewCount']),
            'Description': channel_response['items'][i]['snippet']['description'],
            'Country': channel_response['items'][i]['snippet'].get('country')

        }
    ch_data.append(channel_data)

    return ch_data

def get_videoid_details(pl):
    next_page_token = None
    video_ids = []
    while True:
        playlist_items = youtube.playlistItems().list(part="contentDetails", playlistId=pl, maxResults=50,
                                                      pageToken=next_page_token).execute()
        for item in playlist_items['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = playlist_items.get("nextPageToken")
        if playlist_items.get("nextPageToken") is None:
            break
    return video_ids


# In[10]:


# Function to convert duration
import re


def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                         int(total_seconds % 60))


# In[11]:


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(vids, pl):
    video_data = []

    for i in (vids):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics", id=i).execute()
        for i in range(len(response['items'])):
            video_details = dict(Channel_name=response['items'][i]['snippet']['channelTitle'],
                                 PlaylistId=pl,
                                 Channel_id=response['items'][i]['snippet']['channelId'],
                                 Video_id=response['items'][i]['id'],
                                 Title=response['items'][i]['snippet']['title'],
                                 Tags=response['items'][i]['snippet'].get('tags'),
                                 Thumbnail=response['items'][i]['snippet']['thumbnails']['default']['url'],
                                 Description=response['items'][i]['snippet']['description'],
                                 Published_date=response['items'][i]['snippet']['publishedAt'],
                                 Duration=convert_duration(
                                     response['items'][i]['contentDetails']['duration']),
                                 Views=int(response['items'][i]['statistics']['viewCount']),
                                 Likes=response['items'][i]['statistics'].get('likeCount'),
                                 Dislike_count=response['items'][i]['statistics'].get('dislikeCount', 0),
                                 Comments=response['items'][i]['statistics'].get('commentCount'),
                                 Favorite_count=int(response['items'][i]['statistics']['favoriteCount']),
                                 Definition=response['items'][i]['contentDetails']['definition'],
                                 Caption_status=response['items'][i]['contentDetails']['caption'])
            video_data.append(video_details)
    return video_data


# In[12]:


# Function to get playlist details
def get_playlist(cid):
    playlist_data = []
    token = None
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails", channelId=cid, maxResults=50,
                                           pageToken=token)
        response = request.execute()
        for i in range(len(response['items'])):
            playlist_details_dict = dict(
                pl_itemcount=int(response['items'][i]['contentDetails']['itemCount']),
                pl_id=response['items'][i]['id'],
                pl_title=response['items'][i]['snippet']['title'],
                channel_id=cid,
                pl_description=response['items'][i]['snippet']['description'],
                pl_publishedAt=response['items'][i]['snippet']['publishedAt'])
            playlist_data.append(playlist_details_dict)
        if response.get("nextPageToken") is None:
            break
        token = response.get('nextPageToken')
    return playlist_data


# In[13]:


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(vids):
    comment_data = []
    for i in vids:
        try:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=i,
                                                     maxResults=100).execute()
            for i in range(len(response['items'])):
                data = dict(Comment_id=response['items'][i]['id'],
                            Video_id=response['items'][i]['snippet']['videoId'],
                            Comment_text=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                'textDisplay'],
                            Comment_author=
                            response['items'][i]['snippet']['topLevelComment']['snippet'][
                                'authorDisplayName'],
                            Comment_posted_date=
                            response['items'][i]['snippet']['topLevelComment']['snippet'][
                                'publishedAt'],
                            Like_count=int(
                                response['items'][i]['snippet']['topLevelComment']['snippet'][
                                    'likeCount']),
                            Reply_count=int(response['items'][i]['snippet']['totalReplyCount'])
                            )
                comment_data.append(data)


        except:
            pass
    return comment_data


# # In[14]:
def getcdetails(channel_id):
    c = get_channel_data(channel_id)
    return c


def main(channel_id):
    c = get_channel_data(channel_id)
    vi = get_videoid_details(c[0]['Playlist_id'])
    vd = get_video_details(vi, c[0]['Playlist_id'])
    pl = get_playlist(channel_id)
    cmt = get_comments_details(vi)

    data = {'channel details': c,
            'playlist details': pl,
            'video details': vd,
            'comment details': cmt
            }
    return data



# Moving data in to MongoDB
def tomongo(channel_id):
    x = main(channel_id)
    doc = []
    for i in yt_col.find():
        doc.append(i)
    cn = []
    for j in doc:
        cn.append(j['channel details'][0]['channel_Id'])

    if channel_id not in cn:
        y = yt_col.insert_one(x)
#stage 2

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in yt_col.find():
        ch_name.append(i['channel details'][0]['channel_name'])
    return ch_name


# In[32]:


channel_names()


# In[33]:


# Fetching channel details from MongoDB
def chdata_to_mysql():
    import pprint
    docs = []
    for i in yt_col.find():
        document = i['channel details']
        for doc in document:
            channel_details_to_sql = {
                "Channel_Id": doc['channel_Id'],
                "Channel_name": doc['channel_name'],
                "Subscribers": doc['subscribers'],
                "Total_videos": doc['total_videos'],
                "Views": doc['Views'],
                "Playlist_id": doc['Playlist_id'],
                "Description": doc['Description']}
            docs.append(channel_details_to_sql)

    channel_df = pd.DataFrame.from_dict(docs)
    channel_df.T
    channel_df['Subscribers'] = pd.to_numeric(channel_df['Subscribers'])
    channel_df['Total_videos'] = pd.to_numeric(channel_df['Total_videos'])
    channel_df['Views'] = pd.to_numeric(channel_df['Views'])
    return channel_df


# In[17]:


# mycursor.execute("CREATE DATABASE youtubedb")
# mycursor.execute("CREATE DATABASE ytdatabase")
# mycursor.execute("CREATE DATABASE yt_db1")

# In[34]:


# mycursor.execute("USE yt_db1")
# query = """CREATE TABLE channeldata (
#     channel_id VARCHAR(250),
#     channel_name VARCHAR(250),
#     subscribers INT,
#     Total_videos INT,
#     Views INT,
#     PlaylistId VARCHAR(200),
#     Description VARCHAR(800))"""
# mycursor.execute(query)

# In[35]:


from datetime import datetime, timezone, timedelta


def converting_ytdatetosqlformat(youtube_datetime):
    youtube_format = "%Y-%m-%dT%H:%M:%SZ"
    mysql_format = "%Y-%m-%d %H:%M:%S"
    formatted_datetime = datetime.strptime(youtube_datetime, youtube_format).replace(tzinfo=timezone.utc)
    localtime = timezone(timedelta(hours=5, minutes=30))
    x = formatted_datetime.astimezone(localtime)
    return x.strftime(mysql_format)


# In[36]:


# Fetching playlist data from Mongo DB
def pldata_to_sql():
    k = []
    for i in yt_col.find():
        documents = i['playlist details']
        for j in documents:
            a = j['pl_itemcount']
            b = j['pl_id']
            c = j['pl_title']
            d = j['channel_id']
            e = converting_ytdatetosqlformat(j['pl_publishedAt'])
            playlist_details_to_sql = {'pl_itemcount': a,
                                       'pl_id': b,
                                       'pl_title': c,
                                       'channel_id': d,
                                       'pl_publishedAt': e}
            k.append(playlist_details_to_sql)
    playlist_df = pd.DataFrame(k)
    return playlist_df




# mycursor.execute("USE yt_db1")
# query = """CREATE TABLE playlistdata (
#     pl_itemcount INT,
#     pl_id VARCHAR(255),
#     pl_title VARCHAR(255),
#     channel_id VARCHAR(255),
#     pl_publishedAt DATETIME)"""
# mycursor.execute(query)

# In[38]:


# mycursor.execute("USE yt_db1")
# query = """CREATE TABLE videodetails(
#             Video_id VARCHAR(255),
#             Playlist_id VARCHAR(255),
#             Video_Name VARCHAR(255),
#             Video_Description TEXT,
#             Published_Date DATETIME,
#             View_count INT,
#             Like_count INT,
#             Dislike_count INT,
#             Favourite_count INT,
#             Comment_count INT,
#             Duration INT,
#             Thumbnail VARCHAR(255),
#             Caption_status VARCHAR(255)
#             )"""
# mycursor.execute(query)





def time_to_seconds(time_str):
    try:
        hours, minutes, seconds = map(int, time_str.split(':'))
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    except ValueError:
        # Handle invalid input gracefully
        return None

# Fetching video details from MongoDB
def videodetails_to_mysql():
    v = []
    for i in yt_col.find():
        documents = i['video details']
        for j in documents:
            vid = j['Video_id']
            plid = j['PlaylistId']
            vname = j['Title']
            vdes = j['Description']
            pbldate = converting_ytdatetosqlformat(j['Published_date'])
            views = j['Views']
            likes = j['Likes']
            dislike = j['Dislike_count']
            favrt = j['Favorite_count']
            commt = j['Comments']
            duration = time_to_seconds(j['Duration'])
            thumbnail = j['Thumbnail']
            captionstatus = j['Caption_status']

            video_details_tosql = {'Video_id': vid,
                                   'PlaylistId': plid,
                                   'Title': vname,
                                   'Description': vdes,
                                   'Published_date': pbldate,
                                   'Views': views,
                                   'Likes': likes,
                                   'Dislike_count': dislike,
                                   'Favorite_count': favrt,
                                   'Comments': commt,
                                   'Duration': duration,
                                   'Thumbnail': thumbnail,
                                   'Caption_status': captionstatus
                                   }
            v.append(video_details_tosql)
    video_df = pd.DataFrame(v)
    return video_df

# mycursor.execute("USE yt_db1")
# query = """CREATE TABLE commentdetails(
#                     Comment_Id VARCHAR(255),
#                     Video_Id VARCHAR(255),
#                     Comment_text TEXT,
#                     Comment_author VARCHAR(255),
#                     Comment_posted_date DATETIME)"""
# mycursor.execute(query)


# In[42]:


# Fetching comment details from Mongo DB
def commentdetails_to_sql():
    cmt = []
    for i in yt_col.find():
        documents = i['comment details']
        for j in documents:
            cmtid = j['Comment_id']
            vid = j['Video_id']
            cmtext = j['Comment_text']
            cmauth = j['Comment_author']
            cmtdate = converting_ytdatetosqlformat(j['Comment_posted_date'])

            comment_details_tosql = {'Comment_Id': cmtid,
                                     'Video_id': vid,
                                     'Comment_text': cmtext,
                                     'Comment_author': cmauth,
                                     'Comment_posted_date': cmtdate
                                     }
            cmt.append(comment_details_tosql)
    comment_df = pd.DataFrame(cmt)
    return comment_df

def moveto_sqltable(selected_chname):
    # channel data to mysql
    mycursor.execute("USE yt_db1")
    mycursor.execute("SELECT * from channeldata")
    mysql_data = mycursor.fetchall()
    ch_ids = []
    for i in mysql_data:
        ch_ids.append(i[0])
    import pandas as pd
    channeldetails_df = chdata_to_mysql()
    reqdf = channeldetails_df[channeldetails_df['Channel_name'] == selected_chname]
    chtuple = [tuple(row) for row in reqdf.itertuples(index=False)]
    if chtuple[0][0] not in ch_ids:
        insertquery_chdata = """INSERT INTO channeldata (channel_id, channel_name, subscribers, Total_videos, Views, PlaylistId, Description) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        mycursor.executemany(insertquery_chdata, chtuple)
        connection.commit()

    # playlist data to mysql
    mycursor.execute("SELECT * from playlistdata")
    mysql_pldata = mycursor.fetchall()
    pl_ids = []
    for i in mysql_pldata:
        pl_ids.append(i[1])

    cid = chtuple[0][0]
    playlistdetails_df = pldata_to_sql()
    reqpl_df = playlistdetails_df[playlistdetails_df['channel_id'] == cid]
    pltuple = [tuple(row) for row in reqpl_df.itertuples(index=False)]
    if pltuple[0][1] not in pl_ids:
        insertquery_pldata = """INSERT INTO playlistdata (pl_itemcount, pl_id, pl_title, channel_id, pl_publishedAt) VALUES (%s, %s, %s, %s, %s)"""
        mycursor.executemany(insertquery_pldata, pltuple)
        connection.commit()

    # video details to mysql
    mycursor.execute("SELECT * from videodetails")
    mysql_vddata = mycursor.fetchall()
    vpl_ids = []
    for i in mysql_vddata:
        vpl_ids.append(i[1])
    plid = chtuple[0][5]
    videodetails_df = videodetails_to_mysql()
    reqvd_df = videodetails_df[videodetails_df['PlaylistId'] == plid]
    vdtuple = [tuple(row) for row in reqvd_df.itertuples(index=False)]
    if vdtuple[0][1] not in vpl_ids:
        insertquery_vddata = """INSERT INTO videodetails (Video_id, Playlist_id, Video_Name, Video_Description, Published_Date,View_count,Like_count,Dislike_count,Favourite_count,Comment_count,Duration,Thumbnail,Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        mycursor.executemany(insertquery_vddata, vdtuple)
        connection.commit()

    # comment details to mysql
    mycursor.execute("select distinct Video_Id from commentdetails")
    mysql_vids = mycursor.fetchall()
    cmt_vids = [t[0] for t in mysql_vids]
    rqvids = [v[0] for v in vdtuple]

    commentdetails_df = commentdetails_to_sql()
    reqcmt_df = commentdetails_df[commentdetails_df['Video_id'].isin(rqvids)]
    cmttuple = [tuple(row) for row in reqcmt_df.itertuples(index=False)]

    if cmttuple[0][1] not in cmt_vids:
        insertquery_cmtdetails = """INSERT INTO commentdetails (Comment_Id, Video_id, Comment_text, Comment_author, Comment_posted_date) VALUES (%s, %s, %s, %s, %s)"""
        mycursor.executemany(insertquery_cmtdetails, cmttuple)
        connection.commit()

#s = moveto_sqltable(selected_chname)

