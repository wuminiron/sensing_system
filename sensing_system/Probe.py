#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DataSet
import Region
import datetime
import View

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

    def alarm(self, region, datatype, threshold=-1, timeDelta=5, message="Alert message"):
        regionList = Region.getAll()
        region = strToObject(region, regionList)
        reSensor = []
        res = region.getDevicesUnderRegion()
        for device in res:
            reSensor.extend(device.getSensors())   # get all the sensors
        now = datetime.datetime.now()+datetime.timedelta(hours=timeDelta)
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
                print message   # alarming

        sleep(timeDelta*60*60)  # pause for timeDelta (hour)

    def setRegion(self):
        pass


    def start(self):
        pass

class sendibleTemp(Probe):
    def setRegion(self):
        pass

    def start(self):
        pass