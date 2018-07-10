from Fraud_Detection_API.Util.Utility import getDistanceBetweenPoints
from shapely.geometry import Point

suspendedUsers = None

ID = 'id'
AGE = 'age'
OCCUPATION = 'occupation'
EDUCATION_LEVEL = 'education_level'
AVERAGE_RATING = 'average_rating'
BLACKLISTED_TICKET_COUNT = 'blacklisted_ticket_count'
PEDNING_TICKET_COUNT = 'pending_ticket_count'
ACCEPTED_TICKET_COUNT = 'accepted_ticket_count'
REJECTED_TICKET_COUNT = 'rejected_ticket_count'
WITHDRAWN_TICKET_COUNT = 'withdrawn_ticket_count'
CANCELLED_TICKET_COUNT = 'cancelled_ticket_count'
TICKET_COUNT = 'ticket_count'
LATEST_GIG_LOCATION = 'latest_gig_location'
RATINGS_ABOVE_3_COUNT = 'ratings_above_3_count'
RATING_UNDER_3_COUNT = 'ratings_under_3_count'
AUTH_COUNT = 'authcount'
WORK_COUNT = 'workcount'
LAST_AUDIT_TIME = 'lastaudittime'
LAST_LOCATION = 'lastlocation'
NUM_OF_CLUSTERS = 'cluster_count'
CENTROID = 'centroid'


def approvedTickets(data):
    ruleName = 'Approved Ticket Count'
    totalTickets = data[TICKET_COUNT]
    approvedTicketCount = data[ACCEPTED_TICKET_COUNT]
    if totalTickets is not None and approvedTicketCount is not None:
        ratio = approvedTicketCount / totalTickets
        if ratio > 0.5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False


def averageRating(data):
    ruleName = 'Average Rating'
    averageRating = data[AVERAGE_RATING]
    if averageRating is not None:
        if averageRating > 2.5:
            return ruleName, averageRating, False
        else:
            return ruleName, averageRating, True
    else:
        return ruleName, 0, False


def blockedTickets(data):
    ruleName = 'Blocked Ticket Count'
    totalTickets = data[TICKET_COUNT]
    blacklistedTicketCount = data[BLACKLISTED_TICKET_COUNT]
    if totalTickets is not None and blacklistedTicketCount is not None:
        ratio = blacklistedTicketCount / totalTickets
        if ratio > 0.1:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False


def cancelledTicketCount(data):
    ruleName = 'Canceled Ticket Count'
    totalTickets = data[TICKET_COUNT]
    cancelledTickets = data[CANCELLED_TICKET_COUNT]
    if totalTickets is not None and cancelledTickets is not None:
        ratio = cancelledTickets / totalTickets
        if ratio > 0.5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False


def above3Rating(data):
    ruleName = 'Above 3 rating Ticket Count'
    totalTickets = data[TICKET_COUNT]
    ratingAbove3TicketCount = data[RATINGS_ABOVE_3_COUNT]
    if totalTickets is not None and ratingAbove3TicketCount is not None:
        ratio = ratingAbove3TicketCount / totalTickets
        if ratio > 0.5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False


def under3Rating(data):
    ruleName = 'Under 3 rating Ticket Count'
    totalTickets = data[TICKET_COUNT]
    ratingUnder3Count = data[RATING_UNDER_3_COUNT]
    if totalTickets is not None and ratingUnder3Count is not None:
        ratio = ratingUnder3Count / totalTickets
        if ratio > 0.5:
            return ruleName, ratio, False
        else:
            return ruleName, ratio, True
    else:
        return ruleName, 0, False


def clusterCount(data):
    ruleName = 'CLUSTER_COUNT'
    totalClusters = data[NUM_OF_CLUSTERS]
    if totalClusters is not None:
        if totalClusters < 4:
            return ruleName, totalClusters, False
        else:
            return ruleName, totalClusters, True
    else:
        return ruleName, 0, False


def processClusterData(data):
    ruleName = 'Location Analysis'
    centroid = data[CENTROID]
    latestLocation = data[LAST_LOCATION]
    if latestLocation is not None and centroid is not None:
        distance = getDistanceBetweenPoints(Point(centroid), Point(latestLocation))
        if distance < 5:
            return ruleName, distance, False
        else:
            return ruleName, distance, True
    else:
        return ruleName, 0, False


def authriazationcount(data):
    ruleName = 'Authorization Count'
    auth_count = data[AUTH_COUNT]
    if auth_count is not None:
        if auth_count > 10:
            return ruleName, auth_count, False
        else:
            return ruleName, auth_count, True
    else:
        return ruleName, 0, True


def workCount(data):
    ruleName = 'Authorization Count'
    work_count = data[WORK_COUNT]
    if work_count is not None:
        if work_count > 10:
            return ruleName, work_count, False
        else:
            return ruleName, work_count, True
    else:
        return ruleName, 0, True


def getUserUniversalRuleData(data):
    global suspendedUsers
    suspendedUsers = 0
    ruleList = []
    ruleName, value, redFlag = approvedTickets(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = averageRating(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = blockedTickets(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = cancelledTicketCount(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = above3Rating(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = under3Rating(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = clusterCount(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    #ruleName, value, redFlag = processClusterData(data)
    #ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = authriazationcount(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = workCount(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    print('suspendedusers {0}'.format(suspendedUsers))
    return ruleList

def processOutput(id, ruleName, value, redflag):
    global suspendedUsers
    userReport = dict()
    userReport['Rule Name'] = ruleName
    userReport['Status'] = value
    if redflag:
        raiseRedFlag(id, ruleName)
    userReport['Redflag'] = redflag
    print(userReport)
    suspendedUsers +=1
    return userReport


def raiseRedFlag(id, ruleName):
    print('Notified for id - {0} rulename - {1}'.format(id, ruleName))
