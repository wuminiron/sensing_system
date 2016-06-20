#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime

from Data import Data
from Sensor import Sensor
from Device import Device
from Region import Region
from DataSet import DataSet


from org.cesl.sensordb.client import errorcode


class View(object):
    def __init__(self):
        self.region = None   #  delete all the methods that call Region directly.
        self.dataset = DataSet()
        self.sensor = None  # 支持根据sensorid的查询


    def putRawData(self, sensorID, dataType, sampledTimeStamp, sampledValues, x=0, y=0, z=0):
        self.dataset.putRawData(sensorID, dataType, sampledTimeStamp,  sampledValues, x, y, z)

    def save(self, x=0, y=0, z=0):  #sensor in method tablename= datatype
        self.dataset.save(x, y, z)

    def createTable(self, tablename):
        if not self.dataset.isTableExisted(tablename):   # 是否应该抛出表不存在错误？  no need to judge everytime put data. Maybe the API can do it
            self.dataset.newTable(tablename)

    def getAllRegion(self):    # protected
        return Region.getAll()


    def getAllRegionName(self):
        print "All the region in system are:"
        result = [items.getName() for items in Region.getAll()]
        print result
        return result

    def update(self):
        self.dataset.update()


class SensorIDView(View):
    def sensorIDView(self, sensor, startTime, endTime):
        result = self.dataset.searchData(sensor.getTableName(), sensor.getName(), startTime, endTime)
        printResultSet(result)


class DataView(View):
    def dataView(self,sensorID, datatype, starttime, endtime):

        # regionList = self.getAllRegion()
        # regionObject = strToObject(region, regionList)


        # reSensorList = []
        # for device in regionObject.getDevicesUnderRegion():
        #     reSensorList.extend(device.getSensors())   # get all the sensors
        # sensor = strToObject(sensor, reSensorList)

        rs = self.dataset.searchData("RawData", sensorID, starttime, endtime)

        if not rs or rs.errCode() != errorcode.OK:
            print 'do not find any record'
            return
        rows = rs.getRows(0, rs.size())

        # tmpList = []
        resultList = []
        if rows:
            for i in xrange(len(rows)):
                if rows[i].getString("DataType") == datatype:
                    tmpList = []
                    tmpList.append(rows[i].getSampledTime().strftime('%Y-%m-%d %H:%M:%S'))
                    tmpList.append(rows[i].getInt("Value"))
                    resultList.append(tmpList)
        return resultList




class SensorView(View):   # get all the sensor details under certain region
    def sensorView(self, region):
        regionList = self.getAllRegion()
        region = strToObject(region, regionList)
        reSensor = []
        res = region.getDevicesUnderRegion()
        for device in res:
            reSensor.extend(device.getSensors())   # do not use list.append(), because it focus on single thing.
        result = []
        for sensor in reSensor:
            result.append(sensor.getName() + str(sensor.getAttributes()))
        return result


class DeviceView(View):
    def deviceView(self, region):
        print "Device under {0} are:".format(region)
        regionList = self.getAllRegion()
        region = strToObject(region, regionList)
        res = region.getDevicesUnderRegion()
        result = []
        for device in res:
            result.append(device.getName() + str(device.getAttributes()))
        return result

def strToObject(str, objectList):
    tmpvar_str = 0
    for index, items in enumerate(objectList):
        if items.getName() == str:
            tmpvar_str = index
    return objectList[tmpvar_str]


def printResultSet(rs):
    if not rs or rs.errCode() != errorcode.OK:
        print 'do not find any record'
        return

    print 'row count is %d' % rs.size()

    printRowCount = rs.size()
    rowkeys = rs.getRowKeys(0, printRowCount)
    if rowkeys:
        for i in xrange(len(rowkeys)):
            print 'row key is %s' % rowkeys[i]

    rows = rs.getRows(0, printRowCount)
    if rows:
        for i in xrange(len(rows)):
            loc = rows[i].getSampledLocation()
            print '%s_%d_%f_%f_%f' % (rows[i].getSensorID(), rows[i].getSampledTimestamp(), loc[0], loc[1], loc[2])
            print rows[i].getString("Region")   # getDouble changed to "getString"


def testin():
    Lab1 = Region("Lab1")
    Lab2 = Region("Lab2")
    CO = Data("CO")
    CO2 = Data("CO2")
    Gas = Sensor("001", "table_test_1")
    Car = Device("taxi")
    Car2 = Device("taxi2")
    Car.addHierarchy(Gas, CO)
    Car2.addHierarchy(Gas, CO2)
    Lab1.addDevice(Car)
    Lab2.addDevice(Car2)
    Test = SensorView()
    # Test.createTable("table_test_1")
    # Test2 = DeviceView()
    Wang = Region("Wang")
    Wang.addRegionobject(Lab1)
    Wang.addRegionobject(Lab2)
    # d = datetime.datetime.now()   # need attention. check the three possible format here
    # print d
    for item in range(10):
        d = datetime.datetime.now()
        print d
        Test.saveData(Lab1, Car, Gas, CO, d, 20, 2, 3, 5)
        Test.saveData(Lab2, Car2, Gas, CO2, d, 30, 2, 3, 5)

#if __name__ == "__main__":
    # print "Done"
    # testin()
    # Test = DeviceView()
    #Test3 = DataView()
    #Test3.update()  # first start need to use update() method.
    # Test3.dataView("Wang", "CO", 2)
    #Test3.dataView("Lab1", "CO", 2)
    # Test.getAllRegionName()
    # Test.deviceView("Wang")
    # Test.deviceView("Lab1")
    # Test.deviceView("Lab2")
    # Test2 = SensorView()
    # Test2.sensorView("Lab1")
    # Test2.sensorView("Lab2")
    # Test2.sensorView("Wang")


