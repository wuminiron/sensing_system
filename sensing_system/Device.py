#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Device(object):
    checkDevice = []

    def __init__(self, deviceName, manufacturer, temperatureRange):
        self.deviceName = deviceName
        self.sensors = []   # 聚合关系：sensor
        self.manufacturer = manufacturer
        self.temperatureRange = temperatureRange
        self.region = -1
        Device.checkDevice.append(self)
        self.saveStatus = False

    def addSensorobject(self, sensor):  # double linkage
        tmp = None
        for item in self.sensors:
            if sensor.getName() == item.getName():
                print "Existed SensorID in {0}.".format(self.devicename)
                tmp = -1
        if tmp is None:
            self.sensors.append(sensor)
            sensor.addDevice(self.deviceName)

    def addRegion(self, region):
        self.region = region

    def addHierarchy(self, sensor, dataType):  #新建这些对象应该在View对象中出现,下一步
        self.addSensorobject(sensor)
        sensor.addDataobject(dataType)

    def getName(self):
        return self.deviceName

    def getSensorIDUnderdevice(self):
        reanswer = []
        for sensor in self.sensors:
            reanswer.append(sensor.getName())
        return reanswer

    def getSensors(self):
        return self.sensors

    def getAttributes(self):
        result = dict()
        result["temperatureRange"] = self.temperatureRange
        result["manufacturer"] = self.manufacturer
        return result

    @classmethod
    def getAll(cls):
        return cls.checkDevice

    def getUpperLayer(self):
        return self.region

    def getSaveStatus(self):
        return self.saveStatus

    def setStatusTrue(self):
        self.saveStatus = True