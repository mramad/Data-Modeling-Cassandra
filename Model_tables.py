# Question/Query 1
# Provide name of artist, song  title and song's length in the music app history that was heard during
# sessionId = 338, and itemInSession = 4

# Drop table music_app_hist_sessid if it exists
query = "drop table if exists music_app_hist_sessid_item"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Create table music_app_hist_sessid 
# Primary key Description: Here the Primary Key has two fields: sessionId is the partition key, and itemInSession is clustering key.
# Partitioning is done by sessionId and within that partition, rows are ordered by the itemInSession.
query='CREATE TABLE IF NOT EXISTS music_app_hist_sessid_item'
query= query + "(sessionId int,  itemInSession int,artist text, firstname text, gender text, lastname text, length float,\
                level text, location text, , song text, userId int,\
                PRIMARY KEY (sessionId, itemInSession)    )"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)



# insert data from 'event_datafile_new.csv' into the cassandra table 
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    # iterates through the lines in the file and insert the data into the cassandra table
    for line in csvreader:

        query = "INSERT INTO music_app_hist_sessid_item ( sessionId, itemInSession, artist , firstname , gender ,  lastname \
                , length ,level , location ,   song , userId ) "
        query = query + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        try:
            session.execute(query, (int(line[8]),int(line[3]),line[0], line[1], line[2],  line[4], float(line[5]),
                                    line[6], line[7],  line[9], int(line[10])))
        except Exception as e:
            print(e)



# Check the number of records inserted into the cassandra table to make sure number of 
# records/primary key are correct
query1 = 'select count(1) from music_app_hist_sessid_item '

try:
    rows = session.execute(query1)
    
except Exception as e:
    print(e)
    
for row in rows:
    print (row)
    
    
# Question1 : select name of artist, song title and song's length in the music app history that was heard during
# sessionId = 338, and itemInSession = 4
query2 = "select artist,song,length from music_app_hist_sessid_item where sessionId=338 and itemInSession=4 "

try:
    rows=session.execute(query2)
except Exception as e:
    print(e)

# create DataFrame for displaying the results
# code for the dataframe was obtained through slack workspace
# initiate a list with column names
cols = ['artist', 'song' , 'length']

# initiate an empty list to append the query output to
results = []

# append the query results to the list
for row in rows:
    results.append((row.artist, row.song, row.length))
    
# create dataframe out of the results set
df1 = pd.DataFrame(results, columns = cols)
df1

    
# Question/Query 2
# Provide name of artist, song (sorted by itemInSession) and user (first and last name)
# for userid = 10, sessionid = 182 

# Drop table music_app_hist_userid_sessid if it exists
query = "drop table if exists music_app_hist_userid_sessid"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
# Create table music_app_hist_userid_sessid
# Primary key Description: Here the Primary Key has three fields: userId is the partition key, and sessionId,itemInSession are
# clustering columns.Partitioning is done by userId and within that partition, rows are ordered by the sessionId,itemInSession
query='CREATE TABLE IF NOT EXISTS music_app_hist_userid_sessid'
query= query + "( userId int, sessionId int, itemInSession int, artist text, firstname text, gender text,  lastname text, length float,\
                level text, location text,  song text,\
                PRIMARY KEY (userId, sessionId,itemInSession)     )"

try:
    session.execute(query)
except Exception as e:
    print(e)
    
# insert data from 'event_datafile_new.csv' into the cassandra table 
# file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    # iterates through the lines in the file and insert the data into the cassandra table
    for line in csvreader:
        query = "INSERT INTO music_app_hist_userid_sessid ( userId, sessionId, itemInSession, artist , firstname , gender ,   lastname \
                , length ,level , location ,song   ) "
        query = query + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            session.execute(query, (int(line[10]), int(line[8]),int(line[3]),line[0], \
                                    line[1], line[2], line[4], float(line[5]), line[6], line[7], line[9]))
        except Exception as e:
            print(e)


# Check the number of records inserted into the cassandra table to make sure number \
# of records/primary key are correct
query1 = "select count(1) from music_app_hist_userid_sessid "

try:
    rows=session.execute(query1)
except Exception as e:
    print(e)

for row in rows:
    print (row)
    
# Question 2
# select name of artist, song (sorted by itemInSession) and user (first and last name)\
# for userid = 10, sessionid = 182     
query2 = "select artist,song,firstname,lastname,iteminsession from music_app_hist_userid_sessid \
          where userid=10 and sessionid = 182 "

try:
    rows=session.execute(query2)
except Exception as e:
    print(e)
    
# create DataFrame for displaying the results
# code for the dataframe was obtained through slack workspace
# initiate a list with column names
cols = ['artist', 'song' , 'firstname','lastname']

# initiate an empty list to append the query output to
results = []
# append the query results to the list
for row in rows:
    results.append((row.artist, row.song, row.firstname,row.lastname))
    
# create dataframe out of the results set

df2 = pd.DataFrame(results, columns = cols)
df2
                    
    

# Question 3
# Provide user name (first and last) in music app history who listened to the song 'All Hands Against His Own'

# Drop table music_app_hist_by_song if it exists
query = "drop table if exists music_app_hist_by_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Create table music_app_hist_by_song 
# Primary key Description: Here the Primary Key has two fields: song is the partition key, and userId is the clustering column.\
# Partitioning is done by song and within that partition, rows are ordered by userId.
query='CREATE TABLE IF NOT EXISTS music_app_hist_by_song'
query= query + "(song text,userId int , firstname text,  lastname text, PRIMARY KEY (song,userId)  )"

try:
    session.execute(query)
except Exception as e:
    print(e)
    
# insert data from 'event_datafile_new.csv' into the cassandra table 
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    # iterates through the lines in the file and insert the data into the cassandra table
    for line in csvreader:
        query = "INSERT INTO music_app_hist_by_song ( song,userId, firstname , lastname ) "
        query = query + " VALUES (%s,%s,%s,%s)"
       
        try:
            session.execute(query, (line[9],int(line[10]), line[1],   line[4] ))
        except Exception as e:
            print(e)

# Check the number of records inserted into the cassandra table to make sure number of records/primary key\
# are correct
query1 = "select count(1) from music_app_hist_by_song"

try:
    rows=session.execute(query1)
except Exception as e:
    print(e)

for row in rows:
    print (row)
    
    
# Question 3
# select user name (first and last) in music_app_hist_by_song who listened to the song 'All Hands Against His Own'
query2 = "select firstname,lastname from music_app_hist_by_song where song= 'All Hands Against His Own' "

try:
    rows=session.execute(query2)
except Exception as e:
    print(e)
    
# create DataFrame for displaying the results
# code for the dataframe was obtained through slack workspace
# initiate a list with column names
cols = ['firstname', 'lastname' ]
#initiate an empty list to append the query output to
results = []
# append the query results to the list
for row in rows:
    results.append((row.firstname, row.lastname))
    
# create dataframe out of the results set

df3 = pd.DataFrame(results, columns = cols)
df3



                    
