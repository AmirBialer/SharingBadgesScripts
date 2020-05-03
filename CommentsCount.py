import pandas as pd
import numpy as np
import re



UsersWithId = pd.read_csv('data/UsersWithId.csv')#csv file with names and id, make sure the title in the first row IN the file is same as our file
Newlogs=pd.read_csv('C:/Users/Amir/PycharmProjects/badges_moodle/data/AllData.csv')#log file as downloaded from moodle



def get_deleted_object(data, object):
    deleted_objects = pd.DataFrame(columns={'object_id'})
    for index, row in data.iterrows():
        if(row['Component'] == object +' deleted'):
            object_id = re.search('has deleted the '+object.lower() + ' with id (.+?) in', row['Event name'])
            if object_id:
                found = object_id.group(1)
                deleted_objects = deleted_objects.append({'object_id': found}, ignore_index=True)
    return deleted_objects
"""
the moodle logs keep track of messages which were written and then deleted so we create a file with all deleted messages to be aware
"""

DeletedDiscussions = get_deleted_object(Newlogs, 'Discussion')
DeletedComments = get_deleted_object(Newlogs,'Post')
DeletedDiscussions.to_csv('data/DeletedPosts.csv')

def is_deleted_object(row, object):
    object_id = re.search('has created the ' + object + ' with id (.+?) in', row['Event name'])
    if object_id:
        found = object_id.group(1)
        if object == 'Post' :
            return pd.Series(DeletedComments['object_id']).str.contains(found, regex=False).any()
        else:
            return pd.Series(DeletedDiscussions['object_id']).str.contains(found, regex=False).any()


def mark_rows(data, object):
    for index, row in data.iterrows():
        if (row['Component'] == object+' created'):
            if not is_deleted_object(row, object.lower()):
                data.loc[index, 'counting'] = 1
            else:
                data.loc[index, 'counting'] = 0
        else:
            data.loc[index, 'counting'] = 0
    return data.reset_index()
"""
we keep track of people who start a discussion and people who comment on an existing discussion so these are the two variables
Update_discussion contains all Newlogs and adds a coloumn with 1s if it is indeed a new discussion otherwise zero
Update_comments does the same but for comments
"""
UpdateData_discussions = mark_rows(Newlogs, 'Discussion')
UpdateData_comments = mark_rows(Newlogs, 'Post')

def Summarizer(UpdateData):
    sume = np.sum(UpdateData['counting'])
    s2 = pd.Series(sume, name='counting')
    return pd.concat([s2], axis=1)

def GetDisBadges(UpdateData_discussions):
    for index, row in UpdateData_discussions.iterrows():
        if (row['NewPosts']>40):
            UpdateData_discussions.loc[index,'DiscussionBadge']='Socrates'
        elif (row['NewPosts']>20):
            UpdateData_discussions.loc[index,'DiscussionBadge']='curious'
        elif (row['NewPosts'] > 5):
            UpdateData_discussions.loc[index, 'DiscussionBadge'] = 'inquisitive'
    return UpdateData_discussions

def GetComBadges(UpdateData_comments):
    for index, row in UpdateData_comments.iterrows():
        if (row['Comments']>50):
            UpdateData_comments.loc[index,'CommentsBadge']='Guro'
        elif (row['Comments']>30):
            UpdateData_comments.loc[index,'CommentsBadge']='Champion'
        elif (row['Comments'] > 8):
            UpdateData_comments.loc[index, 'CommentsBadge'] = 'Hero'
    return UpdateData_comments

"""
Summerizing the counts, giving badges as described in the functions above and saving the files
"""
UpdateData_discussions = UpdateData_discussions.groupby('User full name').apply(lambda x: Summarizer(x)).reset_index()
UpdateData_discussions = UpdateData_discussions.drop(['level_1'], axis=1)
UpdateData_discussions=UpdateData_discussions.rename(columns={"User full name": "name", 'counting' : "NewPosts"})
UpdateData_discussions=GetDisBadges(UpdateData_discussions)#If you dont want the badges, delete this line

UpdateData_comments = UpdateData_comments.groupby('User full name').apply(lambda x: Summarizer(x)).reset_index()
UpdateData_comments = UpdateData_comments.drop(['level_1'], axis=1)
UpdateData_comments=UpdateData_comments.rename(columns={"User full name": "name", 'counting':"Comments"})
UpdateData_comments=GetComBadges(UpdateData_comments)#If you dont want the badges, delete this line
UpdateData=pd.merge(UpdateData_discussions,UpdateData_comments,on=['name'],how='left')
UpdateData=pd.merge(UsersWithId,UpdateData,on=['name'],how='left')

def FinalizeFiles(data):
    AllStudents = pd.DataFrame(columns=data.columns)
    for index, row in UsersWithId.iterrows():
        ind = row['name'] == data['name']
        if (ind.any()):
            AllStudents=AllStudents.append(data.loc[ind], ignore_index=True, sort=False)
        else:
            AllStudents = AllStudents.append(pd.Series([row['name'],row['id'],0,'',0,''], index=data.columns), ignore_index=True, sort=False)
    return AllStudents

AllStudents=FinalizeFiles(UpdateData)

#saving the file:
AllStudents.to_csv('data/AllStudentsCommentsStatus.csv', index=False)

