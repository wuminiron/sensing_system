#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DataSet
import Region
import datetime
from View import View, DataView

from time import sleep


def strToObject(str, objectList):
    for index, items in enumerate(objectList):
        if items.getName() == str:
            flag = index
    return objectList[flag]


class Probe(object):
    def __init__(self):
        self.dataset = DataSet()
        self.region = None
        self.regionList = []

    def setRegion(self):
        pass

    def start(self):
        pass

class sendibleTemp(Probe):
    def setRegion(self, *regions):
        regionList = Region.getAll()
        for item in regions:
            self.regionList.append(strToObject(item, regionList))


    def start(self):
        flag = False
        for region in self.regionList:
            deviceList = region.getDevicesUnderRegion()
            for device in deviceList:
                sensorList = device.getSensors()
                for sensor in sensorList:
                    if sensor.getDatatypeUndersensor() == "Temp":
                        tempID = sensor.getName()
                    if sensor.getDatatypeUndersensor() == "H2O":
                        h2oID = sensor.getName()
                    flag = True
        if flag is False:   # search the neighborhood
            for region in self.regionList:
                surrounding = region.getPosition()
                for item in surrounding:
                    deviceList = region.getDevicesUnderRegion()
                    for device in deviceList:
                        sensorList = device.getSensors()
                        for sensor in sensorList:
                            if sensor.getDatatypeUndersensor() == "Temp":
                                tempID = sensor.getName()
                            if sensor.getDatatypeUndersensor() == "H2O":
                                h2oID = sensor.getName()
        tempResult = DataView(tempID, "Temp", "2013-01-01 00:00:00", "2017-01-01 00:00:00")
        h2oResult = DataView(h2oID, "H2O", "2013-01-01 00:00:00", "2017-01-01 00:00:00")
        lenTemp = len(tempResult)
        lenH2O = len(h2oResult)
        sumTemp = sum(tempResult)
        sumH2O = sum(h2oResult)
        temperature = sumTemp / lenTemp
        moisture = sumH2O / lenH2O
        sensidbleTemp = 1.2*temperature + 2*moisture
        return sendibleTemp

class alarm(Probe):
    def start(self, region, datatype, threshold=-1, timeDelta=5, message="Alert message"):
        regionList = Region.getAll()
        region = strToObject(region, regionList)
        reSensor = []
        res = region.getDevicesUnderRegion()
        for device in res:
            reSensor.extend(device.getSensors())  # get all the sensors
        now = datetime.datetime.now() + datetime.timedelta(hours=timeDelta)
        starttime = datetime.datetime.now() + datetime.timedelta(hours=-timeDelta)

        reResult = []
        for sensor in reSensor:
            tmplist = []
            rs = self.dataset.searchData(sensor.getTableName(), sensor.getName(), starttime, now)
            if not rs or rs.errCode() != errorcode.OK:
                print 'do not find any record'
                return
            rows = rs.getRows(0, rs.size())
            if rows:
                for i in xrange(len(rows)):
                    if rows[i].getString("DataType") == datatype:
                        tmplist.append(rows[i].getInt("Value"))
            reResult.append(tmplist)

        for data in reResult:
            if data > threshold:
                print message  # alarming

        sleep(timeDelta * 60 * 60)  # pause for timeDelta (hour)
