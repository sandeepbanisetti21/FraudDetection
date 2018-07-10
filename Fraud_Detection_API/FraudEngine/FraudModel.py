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
        projectReport = dict()
        projectReport['id'] = data['id']
        ruleList = ProjectUniversalRules.getProjectUniversalRuleData(data)
        projectReport['rules'] = ruleList
        projectReport['assigned_user'] = data['assigned_customer_id']
        apiResponse.append(projectReport)
    return apiResponse


def refreshMaterializedView():
    view_merge.mergeViews(ConfigurationParser.config.getPostgresClient(),ConfigurationParser.config.getMonogClient())
    return 'OK'

def detectFraudSingleUser(jsonRequest):
    dataItem = Utility.getOneUserData(jsonRequest['user'])
    apiResponse = []
    usersReport = dict()
    usersReport['id'] = dataItem['id']
    ruleList = UserUniversalRules.getUserUniversalRuleData(dataItem)
    usersReport['rules'] = ruleList
    apiResponse.append(usersReport)
    return apiResponse

def detectFraudSingleProject(jsonRequest):
    dataItems =  Utility.getSingleProjectData(jsonRequest['id'])
    apiResponse = []
    for dataItem in dataItems:
        projectReport = dict()
        projectReport['id'] = dataItem['id']
        ruleList = ProjectUniversalRules.getProjectUniversalRuleData(dataItem)
        projectReport['rules'] = ruleList
        projectReport['assigned_user'] = data['assigned_customer_id']
        apiResponse.append(projectReport)
    return apiResponse

def detectFraudOrgLevel(jsonRequest):
    return
