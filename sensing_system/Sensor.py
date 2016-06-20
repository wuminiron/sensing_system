#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Sensor(object):
    checkSensor = []

    def __init__(self, sensorID,  unit, manufacturer, temperatureRange):
        self.sensorID = sensorID
        # self.tableName = tableName
        self.unit = unit
        self.manufacturer = manufacturer
        self.temperatureRange = temperatureRange
        self.device = -1
        Sensor.checkSensor.append(self)
        self.saveStatus = False
        self.data = []  # 聚合关系：data

    def addDataobject(self, dataIn):
        tmp = None
        for item in self.data:
            if dataIn.getName() == item.getName():
                print "Existed data type in {0}.".format(self.sensorID)
                tmp = -1
        if tmp is None:
            self.data.append(dataIn)
            dataIn.addSensor(self.sensorID)    # newly added data object need to know its sensorID

    def addDevice(self, deviceName):
        self.device = deviceName

    # def getTableName(self):
    #     return self.tableName

    def getName(self):
        return self.sensorID

    def getDatatypeUndersensor(self):
        reanswer = []
        for data in self.data:
            reanswer.append(data.getName())
        return reanswer

    def getAttributes(self):
        result = dict()
        result["unit"] = self.unit
        result["temperatureRange"] = self.temperatureRange
        result["manufacturer"] = self.manufacturer
        return result

    @classmethod
    def getAll(cls):
        return cls.checkSensor

    def getUpperLayer(self):
        return self.device

    def getSaveStatus(self):
        return self.saveStatus

    def setStatusTrue(self):
        self.saveStatus = True