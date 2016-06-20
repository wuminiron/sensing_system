class Data(object):
    checkData = []

    def __init__(self, dataType):
        self.dataType = dataType
        Data.checkData.append(self)
        self.saveStatus = False
        # self.value = value
        self.sensor = -1

    def getName(self):
        return self.dataType

    # def getValue(self):
    #     return self.value

    def addSensor(self, sensorName):
        self.sensor = sensorName

    @classmethod
    def getAll(cls):
        return cls.checkData

    def getUpperLayer(self):
        return self.sensor

    def getSaveStatus(self):
        return self.saveStatus

    def setStatusTrue(self):
        self.saveStatus = True