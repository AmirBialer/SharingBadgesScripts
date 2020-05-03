import pandas as pd
import numpy as np
import re



UsersWithId = pd.read_csv('data/UsersWithId.csv')#csv file with names and id, make sure the title in the first row IN the file is same as our file
Newlogs=pd.read_csv('C:/Users/Amir/PycharmProjects/badges_moodle/data/AllData.csv')#log file as downloaded from moodle

def CountViewsToEachPost(data):
    PostViewCount = pd.DataFrame(columns = ['name', 'postID', 'count'])
    for index, row in data.iterrows():
        if (row['Component'] == 'Discussion viewed'):
            postID=re.search('discussion with id (.+?) in the forum', row['Event name']).group(1)
            a=(PostViewCount['postID']==postID)
            if((PostViewCount['postID']==postID).any()):
                ind=np.array(PostViewCount['postID']==postID).nonzero()[0]
                PostViewCount.loc[ind,'count']+=1
            else:
                PostViewCount = PostViewCount.append(pd.Series([row['User full name'],postID,int(0)],index=PostViewCount.columns), ignore_index=True)
    return PostViewCount

def GetMaxViewsperPerson(data):
    ViewsPerPerson=pd.DataFrame(columns = ['name','id', 'views', 'badge status'])
    for index, row in UsersWithId.iterrows():
        ind=row['name']==data['name']
        if (ind.any()):
            maxViews=np.max(data.loc[ind,'count'].to_numpy())
            badge=''
            if (maxViews>250):
                badge='important'
            elif(maxViews>150):
                badge='popular'
            elif(maxViews>100):
                badge='famous'
            ViewsPerPerson=ViewsPerPerson.append(pd.Series([row['name'],row['id'],maxViews,badge],index=ViewsPerPerson.columns), ignore_index=True)
        else:
            ViewsPerPerson=ViewsPerPerson.append(pd.Series([row['name'],row['id'],0,''],index=ViewsPerPerson.columns), ignore_index=True)
    return ViewsPerPerson


CountViews=CountViewsToEachPost(Newlogs)#Counting how many views to each post
CountViewsPerPerson=GetMaxViewsperPerson(CountViews)#get the post with most views per person
CountViewsPerPerson.to_csv('data/AllStudentsViewsStatus.csv',index=False)

