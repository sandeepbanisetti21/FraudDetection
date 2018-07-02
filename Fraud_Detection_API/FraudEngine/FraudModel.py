from Fraud_Detection_API.Util import ConfigurationParser
from Fraud_Detection_API.Util import Utility
from Fraud_Detection_API.Rules import UserUniversalRules
from Fraud_Detection_API.Rules import ProjectUniversalRules

import Queries
import view_merge

def detectFraudUserLevel(jsonRequest):
    dictCursor = Utility.getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSeclectPgUserView(jsonRequest['count']))
    dataItems = dictCursor.fetchall()
    apiResponse = []
    for data in dataItems:
        usersReport = dict()
        usersReport['id'] = data['id']
        ruleList = UserUniversalRules.getUserUniversalRuleData(data)
        usersReport['rules'] = ruleList
        apiResponse.append(usersReport)
    return apiResponse

def detectFraudProjectLevel(jsonRequest):
    dictCursor = Utility.getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSeclectPgProjectView(jsonRequest['count']))
    dataItems = dictCursor.fetchall()
    apiResponse = []
    for data in dataItems:
        usersReport = dict()
        usersReport['id'] = data['id']
        ruleList = ProjectUniversalRules.getUserUniversalRuleData(data)
        usersReport['rules'] = ruleList
        apiResponse.append(usersReport)
    return apiResponse


def refreshMaterializedView():
    view_merge.mergeViews(ConfigurationParser.config.getPostgresClient(),ConfigurationParser.config.getMonogClient())
    return 'OK'

def detectFraudSingleUser(jsonRequest):
    dictCursor = Utility.getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSelectPgUserById(jsonRequest['user']))
    dataItem = dictCursor.fetchone()
    apiResponse = []
    usersReport = dict()
    usersReport['id'] = dataItem['id']
    ruleList = UserUniversalRules.getUserUniversalRuleData(dataItem)
    usersReport['rules'] = ruleList
    apiResponse.append(usersReport)
    return apiResponse

def detectFraudSingleProject(jsonRequest):
    dictCursor = Utility.getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSelectPgUserById(jsonRequest['id']))
    dataItem = dictCursor.fetchone()
    apiResponse = []
    usersReport = dict()
    usersReport['id'] = dataItem['id']
    ruleList = ProjectUniversalRules.getUserUniversalRuleData(dataItem)
    usersReport['rules'] = ruleList
    apiResponse.append(usersReport)
    return apiResponse

def detectFraudOrgLevel(jsonRequest):
    return
