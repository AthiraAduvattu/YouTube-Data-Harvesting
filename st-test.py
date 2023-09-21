import streamlit as st
import mysql.connector
import pandas as pd
from main import main
from main import getcdetails
from main import tomongo
from main import moveto_sqltable
from main import channel_names

def main1():
    st.title(":red[YouTube Data Harvesting]")
    st.sidebar.header("User Input")
    cid = st.sidebar.text_input("Enter the channel id")
    if st.sidebar.button(":violet[Get channel data]"):
        result = getcdetails(cid)
        st.dataframe(result)
    if st.sidebar.button(":green[Move to Mongo DB]"):
        j = tomongo(cid)
        st.success("Successfully moved to mongodb!!")
    cnames=channel_names()
    k = ['--select--']
    h = [i for i in cnames]
    k.extend(h)
    selected_option = st.selectbox("Select the channel name", [i for i in k])
    if st.button(":blue[Migrate to mysql table]"):
        moveto_sqltable(selected_option)
        st.success("Successfully moved to mysql!!")
    st.subheader(':blue[Channel Data Analysis]')
    #Create select box
    question_tosql = st.selectbox('**Select your Question**',('--select the question--',
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

    # Creat a connection to SQL
    connect_st_sql = mysql.connector.connect(host='localhost', user='root', password='root123', db='yt_db1')
    mycursor = connect_st_sql.cursor()

    # Q1
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        query = """SELECT channeldata.channel_name, videodetails.Video_Name FROM channeldata JOIN videodetails ON channeldata.PlaylistId=videodetails.Playlist_id"""
        mycursor.execute(query)
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        df1.index += 1
        df1
    #Q2
    if question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        query = "SELECT channel_name, Total_videos FROM channeldata ORDER BY Total_videos DESC"
        mycursor.execute(query)
        result2 = mycursor.fetchall()
        df2 = pd.DataFrame(result2, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
        df2.index += 1
        df2
    #Q3
    if question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        query = "SELECT videodetails.Video_Name, videodetails.View_count, channeldata.channel_name FROM videodetails JOIN channeldata ON videodetails.Playlist_id=channeldata.PlaylistId ORDER BY videodetails.View_count DESC LIMIT 10"
        mycursor.execute(query)
        result3 = mycursor.fetchall()
        df3 = pd.DataFrame(result3, columns=['Video Name', 'View Count', 'Channel Name']).reset_index(drop=True)
        df3.index += 1
        df3
    #Q4
    if question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        query = "SELECT channeldata.channel_name,videodetails.Video_Name,videodetails.Comment_count FROM channeldata JOIN videodetails ON videodetails.Playlist_id=channeldata.PlaylistId"
        mycursor.execute(query)
        result4 = mycursor.fetchall()
        df4 = pd.DataFrame(result4, columns=['Channel Name', 'Video Name', 'Comment Count']).reset_index(drop=True)
        df4.index += 1
        df4
    #Q5.
    if question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query="SELECT videodetails.Video_Name,videodetails.Like_count,channeldata.channel_name FROM videodetails JOIN channeldata ON videodetails.Playlist_id=channeldata.PlaylistId ORDER BY videodetails.Like_count DESC"
        mycursor.execute(query)
        result5=mycursor.fetchall()
        df5=pd.DataFrame(result5,columns=['Video Name', 'Like Count', 'Channel Name']).reset_index(drop=True)
        df5.index+=1
        df5
    #Q6.
    if question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query = "SELECT channeldata.channel_name,videodetails.Video_Name,videodetails.Like_count,videodetails.Dislike_count FROM channeldata JOIN videodetails ON videodetails.Playlist_id=channeldata.PlaylistId"
        mycursor.execute(query)
        result6 = mycursor.fetchall()
        df6 = pd.DataFrame(result6, columns=['Channel Name', 'Video Name', 'Like Count', 'Dislike Count']).reset_index(drop=True)
        df6.index += 1
        df6
    #Q7
    if question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query = "SELECT channel_name,Views FROM channeldata"
        mycursor.execute(query)
        result7 = mycursor.fetchall()
        df7 = pd.DataFrame(result7, columns=['Channel Name', 'Total Views']).reset_index(drop=True)
        df7.index += 1
        df7
    #Q8
    if question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        query = "SELECT channeldata.channel_name,videodetails.Published_Date FROM channeldata JOIN videodetails ON videodetails.Playlist_id=channeldata.PlaylistId WHERE EXTRACT(YEAR FROM Published_date) = 2022"
        mycursor.execute(query)
        result8 = mycursor.fetchall()
        df8 = pd.DataFrame(result8, columns=['Channel Name', 'Published Date']).reset_index(drop=True)
        df8.index += 1
        df8
    #Q9.
    if question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query = "SELECT channeldata.channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME(videodetails.Duration))),'%H:%i:%s') AS duration FROM channeldata JOIN videodetails ON videodetails.Playlist_id=channeldata.PlaylistId GROUP by Channel_Name ORDER BY duration DESC"
        mycursor.execute(query)
        result9 = mycursor.fetchall()
        df9 = pd.DataFrame(result9, columns=['Channel Name', 'Average Duration']).reset_index(drop=True)
        df9.index += True
        df9
    #Q10
    if question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query="SELECT channeldata.channel_name,videodetails.Video_Name,videodetails.Comment_count FROM channeldata JOIN videodetails ON videodetails.Playlist_id=channeldata.PlaylistId"
        mycursor.execute(query)
        result10=mycursor.fetchall()
        df10=pd.DataFrame(result10,columns=['Channel Name','Video Name','Comment Count']).reset_index(drop=True)
        df10.index+=1
        df10










if __name__ == "__main__":
    main1()
