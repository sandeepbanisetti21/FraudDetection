import os
import DatabaseClient
import Utility
#from Fraud_Detection_API.Rules.UserUniversalRules import UserRules

postgresUrl = os.environ.get("POSTGRES_URL")
mongoDbUrl = os.environ.get("MONGO_DB_URL")



class Configurations():
    def __int__(self):
        self._mongoClient = None
        self._sqlAlchemySession = None
        self._postgresClient = None
        self._UniversalUserRules = None

    def setMongoClient(self):
        self._mongoClient = DatabaseClient.getMongoDbClient()

    def setSqlAlchemySession(self):
        self._sqlAlchemySession = DatabaseClient.getSqlAlchemySession()

    def setPostgresClient(self):
        self._postgresClient = DatabaseClient.getPostgresClient()

    def getMonogClient(self):
        return self._mongoClient

    def getSqlAlchemySession(self):
        return self._sqlAlchemySession

    def getPostgresClient(self):
        return self._postgresClient
    '''
    def setUniversalUserRules(self):
        self._UniversalUserRules = Utility.methodsWithDecorator(UserRules,'UniversalUser')

    def getUniversalUserRules(self):
        return self._UniversalUserRules
    '''
config = Configurations()
def configure():
    global config
    #config.setMongoClient()
    #config.setSqlAlchemySession()
    config.setPostgresClient()
    #config.setUniversalUserRules()
