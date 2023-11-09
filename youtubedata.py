# import libraries

from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# Youtube api key connection
def Api_connect():
    Api_id = "AIzaSyCrubbE4HlWtD2PGFmOnzldXlHh6-NMkyw"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_id)
    return youtube
youtube = Api_connect()

# get channel information
def get_channel_information(channel_id):
    request = youtube.channels().list(
        part = "snippet,ContentDetails,statistics",
        id =channel_id
    )
    response = request.execute()
    for i in response['items']:
        data = dict(
            channel_name = i['snippet']['title'],
            channel_id = i['id'],
            subscriber = i['statistics']['subscriberCount'],
            views = i['statistics']['viewCount'],
            Total_video = i['statistics']['videoCount'],
            channel_Description = i['snippet']['description'],
            playlist_id = i['contentDetails']['relatedPlaylists']['uploads']


        )
    return data

# get video ids

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id = channel_id , part =  'contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 =  youtube.playlistItems().list(
            part = 'snippet',
            playlistId = playlist_id,
            maxResults = 10,
            pageToken = next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# get video information

def get_video_information(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_id
        )
        response = request.execute()
        for item in response['items']:
            data = dict(channel_Name = item['snippet']['channelTitle'],
                        channel_id =  item['snippet']['channelId'],
                        video_id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail =item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        published_data = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        views = item['statistics'].get('viewCount'),
                        likes = item['statistics'].get('likeCount'),
                        comments = item['statistics'].get('commentCount'),
                        favorite_count = item['statistics']['favoriteCount'],
                        definition = item['contentDetails']['definition'],
                        caption_status =item['contentDetails']['caption']
                )
            video_data.append(data)
    return video_data
    

# get comment infromation

def get_comment_information(video_ids):
    comment_data =[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 10
            )
            response = request.execute()
            for item in response['items']:
                data = dict(
                    Comment_id =  item['snippet']['topLevelComment']['id'],
                    Video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
        pass
    return comment_data 

# get comment information

def get_comment_information(video_ids):
    comment_data =[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 10
            )
            response = request.execute()
            for item in response['items']:
                data = dict(
                    Comment_id =  item['snippet']['topLevelComment']['id'],
                    Video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
        pass
    return comment_data

# get_playlist_details

def get_playlists_details(channel_id):
    next_page_token = None
    playlist_data =[]
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 10,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                playlist_id = item['id'],
                title = item['snippet']['title'],
                Channel_id = item['snippet']['channelId'],
                Channel_name= item['snippet']['channelTitle'],
                publishedAt = item['snippet']['publishedAt'],
                Video_Count = item['contentDetails']['itemCount']
            )
            playlist_data.append(data)
        next_page_token =response.get('nextPageToken') 
        if next_page_token is None:
            break   
    return playlist_data

# MongoDB connection

client = pymongo.MongoClient("mongodb+srv://aswinkasimannan:aswink@cluster0.7fgyt0a.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_data"]

# upload the channel_details into MongoDB

def channel_details(channel_id):
    channel_details = get_channel_information(channel_id)
    playlist_details = get_playlists_details(channel_id)
    video_ids = get_video_ids(channel_id)
    video_info_details = get_video_information(video_ids)
    comment_info_details = get_comment_information(video_ids)

    collection1 = db["channel_details"]
    collection1.insert_one({"channel_information":channel_details,"playlist_information":playlist_details,"video_information":video_info_details,"comment_information":comment_info_details})
    return "upload successfully"

# table creation for channels

def channels_table():
    mydb = psycopg2.connect(host = "localhost",
    user = "postgres",
    password = "aswin",
    database = "youtube_data",
    port = "5432")  
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
        channel_id varchar(80) primary key ,
        subscriber bigint,
        views bigint,
        Total_video int,
        channel_Description text,
        playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")

    ch_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df =pd.DataFrame(ch_list)

    for index,row in df.iterrows ():
        insert_query = '''insert into channels(channel_name,channel_id,subscriber,views,Total_video,channel_Description,playlist_id)
        values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                row['channel_id'],
                row['subscriber'],
                row['views'],
                row['Total_video'],
                row['channel_Description'],
                row['playlist_id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channels values are already inserted")

# create the playlist table
 
def playlists_table():
    mydb = psycopg2.connect(host = "localhost",
    user = "postgres",
    password = "aswin",
    database = "youtube_data",
    port = "5432")  
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''create table if not exists playlists(playlist_id varchar(100) primary key,
                    title varchar(100),
                    Channel_id varchar(100),
                    Channel_name varchar(100),
                    publishedAt timestamp,
                    Video_Count int
                )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")
        
    playlist_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])): # for total playlist
            playlist_list.append(pl_data["playlist_information"][i])
    df1 =pd.DataFrame(playlist_list)

    for index,row in df1.iterrows ():
            insert_query = '''insert into playlists(playlist_id,title, Channel_id,Channel_name ,publishedAt,Video_Count)
            values(%s,%s,%s,%s,%s,%s)'''
            values = (row['playlist_id'],
                    row['title'],
                    row['Channel_id'],
                    row['Channel_name'],
                    row['publishedAt'],
                    row['Video_Count'])
            
            cursor.execute(insert_query,values)
            mydb.commit()

# create the video table

def Video_table():
        mydb = psycopg2.connect(host = "localhost",
        user = "postgres",
        password = "aswin",
        database = "youtube_data",
        port = "5432")  
        cursor = mydb.cursor()

        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query = '''create table if not exists videos(channel_Name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Tags text,
                                                        Thumbnail  varchar(200),
                                                        Description text,
                                                        published_data timestamp,
                                                        Duration interval,
                                                        views bigint,
                                                        likes bigint,
                                                        comments int,
                                                        favorite_count int,
                                                        definition varchar(10),
                                                        caption_status varchar(50)
                )'''
        cursor.execute(create_query)
        mydb.commit()

        video_list = []
        db = client['youtube_data']
        collection1 = db['channel_details']
        for vi_data in collection1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        video_list.append(vi_data["video_information"][i])
        df2 =pd.DataFrame(video_list)

        for index,row in df2.iterrows ():
                insert_query = '''insert into videos(channel_Name,channel_id,video_id,Title,Tags,Thumbnail,Description,published_data,Duration,views,likes,comments,favorite_count,definition,caption_status)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['channel_Name'],
                        row['channel_id'],
                        row['video_id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['published_data'],
                        row['Duration'],
                        row['views'],
                        row['likes'],
                        row['comments'],
                        row['favorite_count'],
                        row['definition'],
                        row['caption_status']
                        )
                
                cursor.execute(insert_query,values)
                mydb.commit()

# create the comment table
    
def comments_table():
    mydb = psycopg2.connect(host = "localhost",
    user = "postgres",
    password = "aswin",
    database = "youtube_data",
    port = "5432")  
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''create table if not exists comments(Comment_id varchar(100) primary key,
                    Video_id varchar(50),
                    Comment_text text,
                    comment_author varchar(150),
                    comment_published timestamp
                )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("comments table already created")

    
    comm_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])): # for total playlist
            comm_list.append(com_data["comment_information"][i])
    df3 =pd.DataFrame(comm_list)


    for index,row in df3.iterrows ():
            insert_query = '''insert into comments(Comment_id,Video_id,Comment_text,comment_author,comment_published)
            values(%s,%s,%s,%s,%s)'''
            values = (row['Comment_id'],
                    row['Video_id'],
                    row['Comment_text'],
                    row['comment_author'],
                    row['comment_published'])
            
            cursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    playlists_table()
    Video_table()
    comments_table()
    return "table created successfully"

def show_channels_table():
    ch_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df =st.dataframe(ch_list)
    return df

def show_playlist_table():
    playlist_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])): # for total playlist
            playlist_list.append(pl_data["playlist_information"][i])
    df1 =st.dataframe(playlist_list)
    return df1

def show_video_table():
    video_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    video_list.append(vi_data["video_information"][i])
    df2 =st.dataframe(video_list)
    return df2

def show_comment_tabel():
    comm_list = []
    db = client['youtube_data']
    collection1 = db['channel_details']
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])): # for total playlist
            comm_list.append(com_data["comment_information"][i])
    df3 =st.dataframe(comm_list)
    return df3


with st.sidebar:
    st.title(":orange[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["channel_id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlist_table()
elif show_table ==":red[videos]":
    show_video_table()
elif show_table == ":blue[comments]":
    show_comment_tabel()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="aswin",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

     
if question == '1. What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, channel_name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select channel_name as ChannelName,total_video as NO_Videos from channels order by total_video desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, published_data as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from published_data) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))