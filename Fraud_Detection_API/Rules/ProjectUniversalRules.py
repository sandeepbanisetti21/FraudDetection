from shapely.geometry import Point

from Fraud_Detection_API.Util.Utility import getDistanceBetweenPoints,getSingleProjectData,getTimeDifference

USER_ID = 'user_id'
ACCEPTED_TICKET_COUNT='accepted_ticket_count'
TICKET_COUNT='ticket_count'
LATEST_GIG_LOCATION='latest_gig_location'
UPLOADED_LOCATION = 'uploaded_location'
TIME_TAKEN = 'time_taken'
NUM_QUESTIONS_ANSWERED = 'question_answered_count'
NUM_QUESTIONS_ASKED = 'questions_asked_count'
ASSIGNMENT_TYPE = 'assignemnet_type'
PROJECT_LOCATION = 'project_location'
TICKET_TIME_ESTIMATE = 'project_time_estimate'
CENTROID = 'centroid'

userData = None

def projectLocation(data):
    ruleName = 'Project Location'
    uploadedLocation = data[UPLOADED_LOCATION]
    projectLocation = data[PROJECT_LOCATION]
    if uploadedLocation is not None and projectLocation is not None:
        ratio = getDistanceBetweenPoints(Point(uploadedLocation),Point(projectLocation))
        if ratio < 5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def centroidDistance(data):
    ruleName = 'Centroid Distance'
    uploadedLocation = data[UPLOADED_LOCATION]
    centroid = userData[CENTROID]
    if uploadedLocation is not None and centroid is not None:
        ratio = getDistanceBetweenPoints(Point(uploadedLocation), Point(centroid))
        if ratio < 5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False

def approvedTickets(data):
    ruleName = 'Approved Ticket Count'
    totalTickets = userData[TICKET_COUNT]
    approvedTicketCount = userData[ACCEPTED_TICKET_COUNT]
    if totalTickets is not None and approvedTicketCount is not None:
        ratio = approvedTicketCount / totalTickets
        if ratio > 0.5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False

def timeEstimate(data):
    ruleName = 'Time Estimate'
    timeTaken= data[TIME_TAKEN]
    ticketTimeEstimate = data[TICKET_TIME_ESTIMATE]
    if timeTaken is not None and ticketTimeEstimate is not None:
        hoursDiff = abs(getTimeDifference(ticketTimeEstimate,timeTaken))
        if hoursDiff > 2:
           return ruleName,hoursDiff,False
        else:
            return ruleName,hoursDiff,True
    else:
        return ruleName,0,False

def questions(data):
    ruleName = 'Questions'
    questionsAnswered = data[NUM_QUESTIONS_ANSWERED]
    quesionsAsked = data[NUM_QUESTIONS_ASKED]
    if questionsAnswered is not None and quesionsAsked is not None:
        ratio = questionsAnswered/quesionsAsked
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,True


def getProjectUniversalRuleData(data):
    global userData
    userData = getSingleProjectData(data[USER_ID])
    ruleList = []
    ruleName,value,redFlag = projectLocation(data)
    ruleList.append(processOutput(data['id'],ruleName,value,redFlag))
    ruleName, value, redFlag = centroidDistance(data)
    ruleList.append(processOutput(data['id'],ruleName,value,redFlag))
    ruleName, value, redFlag = approvedTickets(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = timeEstimate(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag =  questions(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    return ruleList

def processOutput(id, ruleName, value, redflag):
    userReport = dict()
    userReport['Rule Name'] = ruleName
    userReport['Status'] = value
    if redflag:
        raiseRedFlag(id, ruleName)
    userReport['Redflag'] = redflag
    print(userReport)
    return userReport

def raiseRedFlag(id,ruleName):
    print('Notified for id - {0} rulename - {1}'.format(id,ruleName))





