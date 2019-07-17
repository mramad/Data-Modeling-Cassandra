#Drop the created table
query = "drop table music_app_hist_sessid_item"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table music_app_hist_userid_sessid"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query = "drop table music_app_hist_by_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
session.shutdown()
cluster.shutdown()
