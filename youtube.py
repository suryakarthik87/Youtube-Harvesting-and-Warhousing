#------------------------------------PROJECT TITLE: YOUTUBE DATA HARVESTING AND WARHOUSING----------------------------
#YOUTUBE--------> FOR DATA COLLECTION
#MONGODB--------> FOR TEMPORARILY STORE THE DATA
#POSTGRE-SQL----> DATABASE
#STREAMLIT------> USER INTERFACE TO ANYLYSE AND VISUALIZE THE YOUTUBE DATA


#----------------------------------- IMPORTING LIBRARIES------------------------------------
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from PIL import Image
from googleapiclient.errors import HttpError


#-------------------------------------API KEY CONNECTION-------------------------------------

def Api_connect():
    Api_Id = "AIzaSyDqGmf-kqRAEijDH8ZcWpedlZM2b5Hj12Y"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube = Api_connect()


#---------------------------------------------------MONGO-DB CONNECTION---------------------------------
client = pymongo.MongoClient("mongodb+srv://karthik:02121987@cluster0.hgiluow.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_Details"]

#-------------------------------------GET CHANNEL INFORMATION----------------------------
def get_channel_info(channel_id):
    request = youtube.channels().list(
                    part = "snippet,ContentDetails,statistics",
                    id = channel_id
    )
    response = request.execute()  

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                Thumbnail = i['snippet']['thumbnails']['default']['url'])
    return (data)


#-------------------------------------GET VIDEO ID DETAILS---------------------------------
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#---------------------------------------GET VIDEO INFORMATION DETAILS-------------------------
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id = video_id
        )
        response = request.execute()

        for item in response['items']:
                data = dict(Channel_Name = item['snippet']['channelTitle'],
                            Channel_Id = item['snippet']['channelId'],
                            Video_Id = item['id'],
                            Title = item['snippet']['title'],
                            Tags = item['snippet'].get('tags'),
                            Thumbnail = item['snippet']['thumbnails']['default']['url'],
                            Description = item['snippet'].get('description'),
                            Published_date = item['snippet']['publishedAt'],
                            Duration = item['contentDetails']['duration'],
                            Views = item['statistics'].get('viewCount'),
                            Comments = item['statistics'].get('commentCount'),
                            Favorite_Count = item['statistics']['favoriteCount'],
                            Caption_Status = item['contentDetails']['caption'],
                            Like_Count = item['statistics'].get('likeCount')
                            )
                video_data.append(data)
    return video_data


#---------------------------------------GET COMMENT INFORMATION DETAILS---------------------------
def get_comment_info(video_ids):
    Comment_data = []
    try:    
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId =video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
            
                Comment_data.append(data)

    except:
        pass                      
                
    return Comment_data


#--------------------------------------GET PLAYLIST DETAILS-------------------------------------------
def get_playlist_details(channel_id):
    next_page_token = None
    Playlist_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        Published_At = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount'])
            Playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist_data


#----------------------------UPLOAD CHANNELS, VIDEOS, PLAYLIST, COMMENT DETAILS TO MONGO-DB---------------
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll = db["channel_details"]
    pl_details = get_playlist_details(channel_id)
    coll.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_information":com_details})
    return"upload completed sucessfully"

 
#--------------CHANNEL TABLE CREATION AND MIGRATE THE CHANNEL INFORMATION INTO POSTGRE SQL----------------
def channels_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                                        user = "postgres",
                                        password ="karthik",
                                        database = "Youtube_Harvest",
                                        port = "5432")
    cursor = mydb.cursor()

    

  
    create_query = ''' create table if not exists channels(Channel_Name varchar(255),
                                                            Channel_Id varchar(255) primary key,
                                                            Subscribers int,
                                                            Views int,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(255))'''
    cursor.execute(create_query)
    mydb.commit()

    


    single_channel_details = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["channel_information"])
    df_single_channel_details = pd.DataFrame(single_channel_details)

    for index,row in df_single_channel_details.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:

            cursor.execute(insert_query,values) 
            mydb.commit()

        except:

            news = f"You provided channel name {channel_name_s} is already exists"

            return news

   

#--------------PLAYLIST TABLE CREATION AND MIGRATE THE PLAYLIST INFORMATION INTO POSTGRE SQL----------------
def playlist_table(channel_name_s):

    mydb = psycopg2.connect(host="localhost",
                                            user = "postgres",
                                            password ="karthik",
                                            database = "Youtube_Harvest",
                                            port = "5432")
    cursor = mydb.cursor()

    create_query = ''' create table if not exists playlists(Playlist_Id varchar(255) primary key,
                                                            Title varchar(255),
                                                            Channel_Id varchar(255),
                                                            Channel_Name varchar(255),
                                                            Published_At timestamp,
                                                            Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()

    single_playlist_details = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
        single_playlist_details.append(ch_data["playlist_information"])

    df_single_playlist_details = pd.DataFrame(single_playlist_details[0])

    
    for index,row in df_single_playlist_details.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_At,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values =(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Published_At'],
                row['Video_Count'])
        
      

        cursor.execute(insert_query,values) 
        mydb.commit()

              
#--------------VIDEOS TABLE CREATION AND MIGRATE THE VIDEOS INFORMATION INTO POSTGRE SQL----------------
def videos_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                                                user = "postgres",
                                                password ="karthik",
                                                database = "Youtube_Harvest",
                                                port = "5432")
    cursor = mydb.cursor()

    create_query = ''' create table if not exists videos(Channel_Name varchar(255),
                                                            Channel_Id varchar(255),
                                                            Video_Id varchar(255) primary key,
                                                            Title varchar(255),
                                                            Tags text,
                                                            Thumbnail varchar(255),
                                                            Description text,
                                                            Published_date timestamp,
                                                            Duration interval,
                                                            Views int,
                                                            Comments int,
                                                            Favorite_Count int,
                                                            Caption_Status varchar(255),
                                                            Like_Count int
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()

    single_videos_details = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
            single_videos_details.append(ch_data["video_information"])

    df_single_videos_details = pd.DataFrame(single_videos_details[0])

    for index,row in df_single_videos_details.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_date,
                                                Duration,
                                                Views,
                                                Comments,
                                                Favorite_Count,
                                                Caption_Status,
                                                Like_Count
                                                )
                                                    
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values =(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_date'],
                    row['Duration'],
                    row['Views'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Caption_Status'],
                    row['Like_Count']
                    )  
            
            
            cursor.execute(insert_query,values) 
            mydb.commit()

#--------------COMMENTS TABLE CREATION AND MIGRATE THE COMMENTS INFORMATION INTO POSTGRE SQL----------------
def comments_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user = "postgres",
                            password ="karthik",
                            database = "Youtube_Harvest",
                            port = "5432")
    cursor = mydb.cursor()

    create_query = ''' create table if not exists comments(Comment_Id varchar(255) primary key,
                                                            Video_Id varchar(255),
                                                            Comment_Text text,
                                                            Comment_Author varchar(255),
                                                            Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

    single_comments_details = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
            single_comments_details.append(ch_data["comment_information"])

    df_single_comments_details = pd.DataFrame(single_comments_details[0])

    for index,row in df_single_comments_details.iterrows():
                insert_query = '''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                                        
                                                    values(%s,%s,%s,%s,%s)'''
                values =(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'],
                        )  
                
                
                cursor.execute(insert_query,values) 
                mydb.commit()

#------------------------------------------TABLE CREATION---------------------------
def tables(single_channel):
    
    news = channels_table(single_channel)
    if news:
        return news
    else:
        playlist_table(single_channel)
        videos_table(single_channel)
        comments_table(single_channel)

    return "Tables created sucessfully"

   #----------------------------------------SHOW CHANNEL TABLE--------------------------
def show_channel_tables():
    ch_list = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    
    return df

#----------------------------------------SHOW PLAYLIST TABLE--------------------------
def show_playlists_table():
    pl_list = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])     
    df1 = st.dataframe(pl_list)
    
    return df1

#----------------------------------------SHOW VIDEOS TABLE--------------------------
def show_videos_table():
    vi_list = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range (len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])     
    df2 = st.dataframe(vi_list)
    
    return df2

#----------------------------------------SHOW COMMENTS TABLE--------------------------
def show_comments_tables():
    com_list = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range (len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])     
    df3 = st.dataframe(com_list)
    
    return df3


#---------------------------------------CREATING STREAMLIT USER INTERFACE-------------------------
st.set_page_config(layout= "wide")
st.header(":red[You]Tube DATA HARVESTING AND WARHOUSING", divider = 'rainbow')

with st.sidebar:
        selected = option_menu("Main Menu", ["HOME" ,"View the Channel", "Data Harvest", "Transfer to Data Warhouse", "Data Warhouse", "Analyse and Visualize"])


#----------------------------------------SELECT MENU OPTION : HOME--------------------------------------

if selected == "HOME":
    st.subheader(':blue[Domain :] :red[Social] Media')
    st.header(":blue[Introduction:]") 
            
    st.write("""       YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly
            Streamlit application that leverages the power of the Google API to extract valuable 
            information from YouTube channels. The extracted data is then stored in a MongoDB database,
            subsequently migrated to a PostgrSQL data warehouse, and made accessible for analysis and visualize
            within the Streamlit app.""")
    

    st.subheader(":blue[Technologies and Libraries used in the project:]")

    st.write("1. Python")
    st.write("2. Postgre SQL")
    st.write("3. MongoDB")
    st.write("4. Streamlit")
    st.write("5. Pandas")
    st.write("6. Plotly")
    st.write("7. Google API Client")
    st.write("10.Application Programming Interface(API)")
    
    st.subheader(":blue[Features:]")
    st.write(":blue[a. Data collection:]")
    st.write("""Utilize the Google API to retrieve comprehensive data from YouTube channels.
            The data includes information on channels, playlists, videos, and comments.
            By interacting with the Google API, we collect the data and merge it into a JSON file.""")
    
    st.write(":blue[b. Migrating Data to SQL:]" )
    st.write("""The application allows users to migrate data from MongoDB to a SQL data warehouse.
                Users can choose which channel's data to migrate. To ensure compatibility with a structured
                format, the data is cleansed using the powerful pandas library. Following data cleaning,
                the information is segregated into separate tables, including channels, playlists, videos,
                and comments, utilizing SQL queries.""")
    
    st.write(":blue[c. Data Analysis and Visualization:]")
    st.write("""The project provides comprehensive data analysis capabilities using Plotly and Streamlit.
                With the integrated Plotly library, users can create interactive and visually appealing
                charts and graphs to gain insights from the collected data.""")
    st.write(":blue[d. Channel Analysis:]")
    st.write("""Channel analysis includes insights on playlists, videos, subscribers, views, likes,
                comments, and durations. Gain a deep understanding of the channel's performance and
                audience engagement through detailed visualizations and summaries.""")
    st.write(":blue[e. Video Analysis:]")
    st.write("""Video analysis focuses on views, likes, comments, and durations, enabling both an overall
                channel and specific channel perspectives. Leverage visual representations and metrics to
                extract valuable insights from individual videos. Utilizing the power of Plotly, users can
                create various types of charts, including line charts, bar charts, scatter plots, pie charts,
                and more. These visualizations enhance the understanding of the data and make it easier to
                identify patterns, trends, and correlations. The Streamlit app provides an intuitive interface
                to interact with the charts and explore the data visually. Users can customize the
                visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis.
                With the combined capabilities of Plotly and Streamlit, the Data Analysis section empowers users
                to uncover valuable insights and make data-driven decisions.""")


#-------------------------------------------SELECT OPTION MENU : VIEW THE CHANNEL---------------------------------

if selected == "View the Channel":
    channel_id = st.text_input("Enter the YouTube Channel ID to view:")
    if st.button("Show"):
        info = get_channel_info(channel_id)
        st.write("**:blue[Thumbnail]**:")
        st.image(info.get("Thumbnail"))
        st.write("**:blue[Channel_Name]**:",info["Channel_Name"])
        st.write("**:blue[Channel_Description]**:",info["Channel_Description"])
        st.write("**:blue[Total_Videos]**:",info["Total_Videos"])
        st.write("**:blue[Subscribers]**:",info["Subscribers"])
        st.write("**:blue[Views]**:",info["Views"])


#------------------------------------SELECT OPTION MENU : DATA HARVEST------------------------------------------------

if  selected == "Data Harvest":
    try:
        channel_id = st.text_input("Enter the YouTube Channel ID to Store:")
        st.button("Extract and Store")
        ch_ids = []
        db = client["Youtube_Details"]
        coll = db["channel_details"]
        for ch_data in coll.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])

        if channel_id in ch_ids:
            st.success("Channel Details of the Given channel id is already exists")

        else:
            insert = channel_details(channel_id)
            st.success(insert)

    except HttpError as e:
        if e.resp.status == 403 and e.error_details[0]["reason"] == 'API Quota exceeded':
                    st.error(" API Quota exceeded. Please try again later.")
    except:
        st.error("Please ensure to give valid channel ID")


#-------------------------------------SELECT OPTION MENU : TRANSFER TO DATA WARHOUSE-------------------------------


if  selected == "Transfer to Data Warhouse":
    
    all_channels = []
    db = client["Youtube_Details"]
    coll = db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
    unique_channel = st.selectbox("Select the Channel to Store in Data Warhouse",all_channels)


    if st.button("Store in Warhouse"):
        Table = tables(unique_channel)
        st.success(Table)



#-------------------------------------SELECT OPTION MENU : DATA WARHOUSE------------------------------------------

if selected == "Data Warhouse":
    st.header("Data Warhouse")
    show_table =st.selectbox("Select the Data Warhouse to view",["CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"])
   
    if show_table == "CHANNELS":
        show_channel_tables()

    elif show_table == "PLAYLISTS":
        show_playlists_table()    

    elif show_table == "VIDEOS":
        show_videos_table()

    elif show_table == "COMMENTS":
        show_comments_tables()

        
#---------------------------- POSTGRE SQL DATABASE CONNECTION TO RETRIVE THE DATA FOR ALL QUERIES----------------
mydb = psycopg2.connect(host="localhost",
                        user = "postgres",
                        password ="karthik",
                            database = "Youtube_Harvest",
                        port = "5432")
cursor = mydb.cursor()


#----QUESTION SELETION TO VIEW THE CHANNELS, PLAYLISTS, VIDEOS, COMMENTS INFORMATION TO ANALYSE AND VISUALIZE ALL THE QUERIES
if selected == "Analyse and Visualize":

    question = st.selectbox("Select Your Queries to Analyse and Visualize:", ("1. What are the names of all the videos and their corresponding channels?",
                                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

    #QUESTION 1:

    if question == "1. What are the names of all the videos and their corresponding channels?":
        query1 = ''' select title as videos,channel_name as channelname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1 = cursor.fetchall()
        df = pd.DataFrame(t1, columns= ["Video Name", "Channel Name"])
        st.write(":blue[What are the names of all the videos:]")
        st.write(df)
        
        
    #QUESTION 2:

    elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
        query2 = ''' select channel_name as channelname, total_videos as No_of_Videos from channels
                        order by total_videos desc'''
        cursor.execute(query2)
        mydb.commit()
        t2 = cursor.fetchall()
        df2 = pd.DataFrame(t2, columns= ["Channel Name", "No. of Videos"])  
        st.write(":blue[Which channels have the most number of videos:]")
        st.write(df2)
        fig = px.bar(df2,
                        x="Channel Name",
                        y="No. of Videos",
                        orientation='v',
                        color= "Channel Name"                  
                    )
        st.plotly_chart(fig, width=600, height=600)       
            
        
    #QUESTION 3:

    elif question == "3. What are the top 10 most viewed videos and their respective channels?":
        query3 = ''' select views as views, channel_name as channelname, title as videotitle from videos
                        where views is not null order by views desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3, columns= ["Views", "Channel Name","Video Title"  ])
        st.write(":blue[What are the top 10 most viewed videos:]")
        st.write(df3)
        fig = px.bar(df3, y="Views", x="Video Title",color = "Channel Name")
        st.plotly_chart(fig,use_container_width=True)

    #QUESTION 4:
    elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
        query4 = ''' select comments as No_of_Comments, title as videotitle from videos where comments is not null'''
        cursor.execute(query4)
        mydb.commit()
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4, columns= ["No of Comments", "Video Title"])
        st.write(":blue[How many comments were made on each video:]")
        st.write(df4)

    #QUESTION 5:

    elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = ''' select title as videotitle, channel_name as channelname, Like_Count as likecount
                        from videos where Like_Count is not null order by Like_Count desc '''
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5, columns= ["Video Title", "Channel Name", "Likes"])
        st.write(":blue[Which videos have the highest number of likes:]")
        st.write(df5)
        fig = px.bar(df5, y="Likes", x="Video Title",color = "Channel Name",orientation='v')
        st.plotly_chart(fig)


    #QUESTION 6:

    elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = ''' select Like_Count as likes, title as videotitle from videos '''
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6, columns= ["Likes","Video Title"])
        st.write(":blue[What is the total number of likes and dislikes for each video:]")
        st.write(df6)

    #QUESTION 7:

    elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = ''' select views as views, channel_name as channelname from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7, columns= ["Total Views","Channel Name"])
        st.write(":blue[What is the total number of views for each channel:]")
        st.write(df7)
        fig=px.bar(df7, x="Channel Name", y="Total Views",color="Channel Name",orientation='v' )                    
        st.plotly_chart(fig,use_container_width=True)

    #QUESTION 8:

    elif question =="8. What are the names of all the channels that have published videos in the year 2022?":
        query8 = ''' select title as videotitle, Published_date as releasedate, channel_name as channelname from videos
                        where extract(year from Published_date)=2022 '''
        cursor.execute(query8)
        mydb.commit()
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8, columns= ["Video Title","Published Date", "Channel Name"])
        st.write(":blue[Videos published in year 2022:]")
        st.write(df8)

    #QUESTION 9:

    elif question =="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = ''' select channel_name as channelname, AVG(Duration) as average_duration from videos
                        group by channel_name  '''
        cursor.execute(query9)
        mydb.commit()
        t9 = cursor.fetchall()
        df9 = pd.DataFrame(t9, columns= ["channelname","Average_Duration"])
        

        T9 = []
        for index, row in df9.iterrows():
            channel_title = row["channelname"]
            average_duration = row["Average_Duration"]
            average_duration_str = str(average_duration)
            T9.append(dict(channeltitle = channel_title, avgduration = average_duration_str))
        df1 = pd.DataFrame(T9)
        st.write(":blue[What is the average duration of all videos in each channel:]")
        st.write(df1)
        

    #QUESTION 10:

    elif question =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = ''' select title as videotitle, channel_name as channelnames, comments as comments from videos
                        where comments is not null order by comments desc  '''
        cursor.execute(query10)
        mydb.commit()
        t10 = cursor.fetchall()
        df10 = pd.DataFrame(t10, columns= ["Video Title","Channel_Name","Comments"])
        st.write(":blue[Which videos have the highest number of comments:]")
        st.write(df10)
        fig=px.bar(df10, x="Video Title", y="Comments", color="Channel_Name")
                    
        st.plotly_chart(fig)
    #-------------------------------------------------------END OF THE PROJECT----------------------------------------------------------
