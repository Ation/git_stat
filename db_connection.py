from sqlalchemy import create_engine

def CreateEngine():
    db_user_name = 'gitstat'
    db_password = 'gitstat'
    db_host = 'localhost'
    db_name = 'gitstat'

    # return create_engine('mysql+pymysql://{}:{}@{}/{}'.format(db_user_name, db_password, db_host, db_name), echo=False)
    return create_engine('postgresql://gitstats:gitstats@192.168.0.100/git_stats', echo=False)
