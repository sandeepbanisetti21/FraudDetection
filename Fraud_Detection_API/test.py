from Fraud_Detection_API.FraudEngine import FraudModel
from Fraud_Detection_API.Rules.UserUniversalRules import UserRules
import inspect

def methodsWithDecorator(cls, decoratorName):
    methods = []
    sourcelines = inspect.getsourcelines(cls)[0]
    for i,line in enumerate(sourcelines):
        line = line.strip()
        if line.split('(')[0].strip() == '@'+decoratorName: # leaving a bit out
            nextLine = sourcelines[i+1]
            name = nextLine.split('def')[1].split('(')[0].strip()
            methods.append(name)
    return methods

if __name__ == '__main__':
    methods = methodsWithDecorator(UserRules,'UniversalUser')
    print(methods)
