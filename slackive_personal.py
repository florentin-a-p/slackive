######## I call this personal because this is optimized for the user token, not the bot token ########

import os
import time
import re
import slack
from dotenv import load_dotenv
from pathlib import Path
import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
# from pandas.io.json import json_normalize

"""
private_channel
public_channel
mpim
im
"""

def get_channels(client,limit=200, channel_type='private_channel'):
    df = pd.DataFrame()
    next_cursor=''
    
    temp = client.conversations_list(cursor=next_cursor,limit=limit,types=channel_type).data

    if temp.get('response_metadata'):
#         print('initial_has_response_metadata')
        next_cursor = temp['response_metadata']['next_cursor']
    else:
#         print('initial_no_response_metadata')
        next_cursor = ''
    
#     print('initial_next_cursor',next_cursor)
    temp = pd.json_normalize(temp,record_path='channels',meta=['ok','response_metadata'])
    df = df.append(temp)
    
    while len(next_cursor)>0: # iterate through all pages
        temp = client.conversations_list(cursor=next_cursor,limit=limit,types=channel_type).data
        if temp.get('response_metadata'):
#             print('has_response_metadata')
            next_cursor = temp['response_metadata']['next_cursor']
        else:
#             print('no_response_metadata')
            next_cursor = ''
#         print('next_cursor',next_cursor)
        temp = pd.json_normalize(temp,record_path='channels',meta=['ok','response_metadata'])
        df = df.append(temp)
#         print('success!!')
    
    next_cursor = ''
#     print('gettign to next channel')
    return df.dropna(subset=['id']).drop_duplicates(subset=['id']).reset_index(drop=True)


def get_list_of_channels(df_channels):
    list_channels = list(df_channels['id'])
    return list_channels


def get_channel_conversations(client, list_channel, limit=200):
    df = pd.DataFrame()
    next_cursor = ''

    for channel in list_channel:
        temp = client.conversations_history(limit=limit,cursor=next_cursor,channel=channel).data
        if temp.get('response_metadata'):
#             print('initial_has_response_metadata')
            next_cursor = temp['response_metadata']['next_cursor']
        else:
#             print('initial_no_response_metadata')
            next_cursor = ''
#         print('initial_next_cursor',next_cursor)
        temp = pd.json_normalize(temp,record_path='messages',meta=['ok',
                                                         'has_more', 
                                                         'pin_count', 
                                                         'channel_actions_ts', 
                                                         'channel_actions_count'])
        temp['channel'] = channel
        df = df.append(temp)
#         print('initial',channel,next_cursor)

        while len(next_cursor)>0: # iterate through all pages
            temp = client.conversations_history(limit=limit,cursor=next_cursor,channel=channel).data
            if temp.get('response_metadata'):
#                 print('has_response_metadata')
                next_cursor = temp['response_metadata']['next_cursor']
            else:
#                 print('no_response_metadata')
                next_cursor = ''
#             print('next_cursor',next_cursor)
            temp = pd.json_normalize(temp,record_path='messages',meta=['ok',
                                                             'has_more', 
                                                             'pin_count', 
                                                             'channel_actions_ts', 
                                                             'channel_actions_count'])
            temp['channel'] = channel
            df = df.append(temp)
#             print('success!!',channel,next_cursor)

        next_cursor = ''
#         print('gettign to next channel')

    df = df.drop_duplicates(subset=['client_msg_id','ts','user']).reset_index(drop=True)
    return df


def get_channel_replies(client, channel, thread_ts, limit, next_cursor):
    result = pd.DataFrame()
#     print(channel,thread_ts)
    temp = client.conversations_replies(channel=channel,cursor=next_cursor,limit=limit,ts=thread_ts).data
    if temp.get('response_metadata'):
#         print('initial_has_response_metadata')
        next_cursor = temp['response_metadata']['next_cursor']
    else:
#         print('initial_no_response_metadata')
        next_cursor = ''
#     print('initial_next_cursor',next_cursor)
    temp = pd.json_normalize(temp,record_path='messages',meta=['ok','has_more'])
    temp['channel'] = channel
    result = result.append(temp)
#     print('initial',channel,next_cursor)

    while len(next_cursor)>0: # iterate through all pages
        temp = client.conversations_replies(channel=channel,cursor=next_cursor,limit=limit,ts=thread_ts).data
        if temp.get('response_metadata'):
#             print('has_response_metadata')
            next_cursor = temp['response_metadata']['next_cursor']
        else:
#             print('no_response_metadata')
            next_cursor = ''
#         print('next_cursor',next_cursor)
        temp = pd.json_normalize(temp,record_path='messages',meta=['ok','has_more'])
        temp['channel'] = channel
#         result = result.append(temp)
        print('success!!',channel,next_cursor)
    
    next_cursor = ''
#     print('gettign to next channel')
    return result


def get_channel_complete_conversations(client, list_channel, limit): #concat get_conversations and get_replies
    df_conversations = get_channel_conversations(client, list_channel, limit=200)
    temp_replies = df_conversations.dropna(subset=['thread_ts'])[['channel','thread_ts']] \
                                    .reset_index(drop=True) \
                                    .drop_duplicates()
    
    df_replies = pd.DataFrame()
    next_cursor = ''
    
    for i,row in temp_replies.iterrows():
        result = get_channel_replies(client,row['channel'],row['thread_ts'],limit,next_cursor)
        df_replies = df_replies.append(result)
    
    df_replies = df_replies.sort_values(by='ts') \
                            .reset_index(drop=True) \
                            .drop_duplicates(subset=['client_msg_id','ts','user']) \
                            .reset_index(drop=True)
                                                 
    df_final = pd.concat([df_conversations,df_replies],axis=0)\
                        .drop_duplicates(subset=['client_msg_id','ts','user'])\
                          .reset_index(drop=True)
    return df_final


def save_to_file(data,file_name,file_type='json'):
    if file_type=='json':
        data.to_json('data/'+file_name)
    elif file_type=='csv':
        data.to_csv('data/'+file_name)
    else:
        print('invalid data type, please pass either json or csv!!')
    pass



