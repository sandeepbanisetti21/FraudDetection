ID='id'
AGE='age'
OCCUPATION='occupation'
EDUCATION_LEVEL='education_level'
AVERAGE_RATING='average_rating'
BLACKLISTED_TICKET_COUNT='blacklisted_ticket_count'
PEDNING_TICKET_COUNT='pending_ticket_count'
ACCEPTED_TICKET_COUNT='accepted_ticket_count'
REJECTED_TICKET_COUNT='rejected_ticket_count'
WITHDRAWN_TICKET_COUNT='withdrawn_ticket_count'
CANCELLED_TICKET_COUNT='cancelled_ticket_count'
TICKET_COUNT='ticket_count'
LATEST_GIG_LOCATION='latest_gig_location'
RATINGS_ABOVE_3_COUNT='ratings_above_3_count'
RATING_UNDER_3_COUNT='ratings_under_3_count'

def approvedTickets(data):
    ruleName = 'Approved Ticket Count'
    totalTickets = data[TICKET_COUNT]
    approvedTicketCount = data[ACCEPTED_TICKET_COUNT]
    if totalTickets is not None and approvedTicketCount is not None:
        ratio = approvedTicketCount/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def averageRating(data):
    ruleName = 'Average Rating'
    averageRating = data[AVERAGE_RATING]
    if averageRating is not None:
        if averageRating > 2.5:
            return ruleName,averageRating,False
        else:
            return ruleName,averageRating,True
    else:
        return ruleName,0,False

def blockedTickets(data):
    ruleName = 'Blocked Ticket Count'
    totalTickets = data[TICKET_COUNT]
    blacklistedTicketCount = data[BLACKLISTED_TICKET_COUNT]
    if totalTickets is not None and blacklistedTicketCount is not None:
        ratio = blacklistedTicketCount/totalTickets
        if ratio > 0.1:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def cancelledTicketCount(data):
    ruleName = 'Canceled Ticket Count'
    totalTickets = data[TICKET_COUNT]
    cancelledTickets = data[CANCELLED_TICKET_COUNT]
    if totalTickets is not None and cancelledTickets is not None:
        ratio = cancelledTickets/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def above3Rating(data):
    ruleName = 'Above 3 rating Ticket Count'
    totalTickets = data[TICKET_COUNT]
    ratingAbove3TicketCount = data[RATINGS_ABOVE_3_COUNT]
    if totalTickets is not None and ratingAbove3TicketCount is not None:
        ratio = ratingAbove3TicketCount/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def under3Rating(data):
    ruleName = 'Under 3 rating Ticket Count'
    totalTickets = data[TICKET_COUNT]
    ratingUnder3Count = data[RATING_UNDER_3_COUNT]
    if totalTickets is not None and ratingUnder3Count is not None:
        ratio = ratingUnder3Count/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def clusterCount(data):
    ruleName = 'Approved Ticket Count'
    totalTickets = data[TICKET_COUNT]
    approvedTicketCount = data[ACCEPTED_TICKET_COUNT]
    if totalTickets is not None and approvedTicketCount is not None:
        ratio = approvedTicketCount/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def processClusterData(data):
    ruleName = 'Approved Ticket Count'
    totalTickets = data[TICKET_COUNT]
    approvedTicketCount = data[ACCEPTED_TICKET_COUNT]
    if totalTickets is not None and approvedTicketCount is not None:
        ratio = approvedTicketCount/totalTickets
        if ratio > 0.5:
           return ruleName,ratio,False
        else:
            return ruleName,ratio,True
    else:
        return ruleName,0,False

def getUserUniversalRuleData(data):
    ruleList = []
    ruleName,value,redFlag = approvedTickets(data)
    ruleList.append(processOutput(data['id'],ruleName,value,redFlag))
    ruleName, value, redFlag = averageRating(data)
    ruleList.append(processOutput(data['id'],ruleName,value,redFlag))
    ruleName, value, redFlag = blockedTickets(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = cancelledTicketCount(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag =  above3Rating(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    ruleName, value, redFlag = under3Rating(data)
    ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    # ruleName, value, redFlag = approvedTickets(data)
    # ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
    # ruleName, value, redFlag = averageRating(data)
    # ruleList.append(processOutput(data['id'], ruleName, value, redFlag))
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





