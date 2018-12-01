# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 02:44:25 2018

@title: Requests from Newsapi to get automated retrieving of news about certain topics.


@author: avaca
"""


from newsapi import NewsApiClient
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import pandas as pd
from datetime import datetime
import re
from pytz import utc



key = ''

newsapi = NewsApiClient(key)


log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.INFO)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

'''
todo esto de aqui arriba es para poder luego ejecutar el programa cada x segundos
'''

def clean_dates(dates):
    
    lista = []
    
    for date in dates:
        
        new = re.sub('T', ' ', date)
        
        new1 = re.sub('Z', '', new)
        
        new2 = datetime.strptime(new1, '%Y-%m-%d %H:%M:%S')
        
        lista.append(new2)
        
    return lista   

    

def get_news_data(queries = ['bitcoin', 'ethereum', 'crypto', 'cryptocoins', 'market', 'economy']):
    
    '''
    outputs a dictionary with the date, the source, the title and the content 
    of all the news related to the query. 
    The values of the query are only manipulated inside the function and not with parameters included in get_news_data
    because the querys will be all the time the same, however this can be perfectly adapted if necessary for each certain data retrieving task
    '''
    for query in queries:

        top_headlines = newsapi.get_top_headlines(q = query, 
                                              sources = 'bbc-news, the-verge, crypto-coins-news, hacker-news, bloomberg',                                         
                                              language = 'en')
        
        articles = top_headlines['articles']
        
        text_date = {'query': [],
                     'date': [],
                     'source': [],
                     'title':[],
                     'text': []}
        
        for article in articles:
            text_date['query'].append(query)
            text_date['date'].append(article['publishedAt'])
            text_date['source'].append(article['source']['id'])
            text_date['title'].append(article['title'])
            text_date['text'].append(article['content'])
            
        new_df = pd.DataFrame(text_date)

        with open(r'news_newsapi_bitcoin.txt', 'r+', encoding = 'utf-8') as f:
        
                existing_df = pd.read_csv(f, encoding = 'utf-8')
                
                existing_df.drop('Unnamed: 0', axis = 1, inplace = True)
            
                big_df = pd.concat([existing_df, new_df])
                
                big_df.drop_duplicates(subset = ['date', 'text'], inplace = True, keep = 'last')
                
                new_dates = clean_dates(big_df['date'])
                
                big_df.drop('date', axis = 1, inplace = True)
                
                big_df['date'] = new_dates
                
                big_df['date'] = pd.to_datetime(big_df['date'])
                
                big_df.sort_values(by = ['date'], inplace = True, ascending = True)
                
                big_df.to_csv('news_newsapi_bitcoin.txt', sep = ',', encoding = 'utf-8')
                
                f.close()
                
    print(big_df)
    
    print("todo funciona según lo previsto, ya te puedes ir a dormir, PESAO")


def main():

    """Run get_news_data() at the interval of every x seconds."""
    
    scheduler = BlockingScheduler(timezone=utc)
    
    scheduler.add_job(get_news_data, 'interval', seconds=120)
    
    try:
        scheduler.start()
        
    except (KeyboardInterrupt, SystemExit):
        
        pass


if __name__ == '__main__':
    main()



        
        
    
    
    
    
    
    
    
    
    
    
    