
def getSeclectPgUserView(count):
    SELECT_FROM_PG_USER_VIEW = 'Select * from user_stats_view where ticket_count is not null limit '
    return SELECT_FROM_PG_USER_VIEW+str(count)

def getSelectPgUserById(id):
    SELECT_FROM_PG_USER_VIEW = 'Select * from user_stats_view WHERE id = '
    statement=  SELECT_FROM_PG_USER_VIEW + str(id)
    return statement

def getSeclectPgProjectView(count):
    SELECT_FROM_PG_PROJECT_VIEW = 'Select * from project_stats_view limit '
    statement =  SELECT_FROM_PG_PROJECT_VIEW+str(count)
    return statement

def getSelectPgProjectById(id):
    SELECT_FROM_PG_PROJECT_VIEW = 'Select * from project_stats_view WHERE ID = '
    return SELECT_FROM_PG_PROJECT_VIEW + str(id)
