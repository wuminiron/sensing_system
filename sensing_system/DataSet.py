#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import calendar
import time

from Data import Data
from Sensor import Sensor
from Device import Device
from Region import Region


from org.cesl.sensordb.client.connection import Connection
from org.cesl.sensordb.client import errorcode

class DataSet(object):
    conn = Connection()
    host = "127.0.0.1"
    port = 6677

    def newTable(self, tableName):
        DataSet.conn.connect(DataSet.host, DataSet.port)
        DataSet.conn.createTable(tableName)
        DataSet.conn.close()

    def isTableExisted(self, tableName):
        DataSet.conn.connect(DataSet.host, DataSet.port)
        result = DataSet.conn.isExisted(tableName)
        DataSet.conn.close()
        return result

    # def deleteTable(self, tableName):
    #     DataSet.conn.connect(DataSet.host, DataSet.port)
    #     if DataSet.isTableExisted(tableName):
    #         result = self.conn.dropTable(tableName)
    #         DataSet.conn.close()
    #         return result
    #     else:
    #         print 'No such table existed'
    #         DataSet.conn.close()
    #         return 0

    def putRawData(self,sensorID, dataType, sampledTimeStamp,  sampledValues, x, y, z):
        self.insertData("RawData", sensorID, sampledTimeStamp, x, y, z, {"Value": sampledValues, "DataType": dataType})

    def insertData(self, tableName, sensorID, d,  x, y, z, sampledValues):  #  x,y,z 坐标默认为0，即不支持空间查询
        DataSet.conn.connect(DataSet.host, DataSet.port)
        ts = calendar.timegm(d.timetuple())
        # ts = ts * 1000 + (d.microsecond / 1000)
        print 'inserted %f' % (ts,)
        DataSet.conn.put(tableName, sensorID, ts, x, y, z, sampledValues)   #
        print '%s_%s_%f' % (tableName, sensorID, ts)
        DataSet.conn.close()

    def searchData(self, tableName, sensorID, startTime, endTime):
        DataSet.conn.connect(DataSet.host, DataSet.port)
        result = DataSet.conn.getBySensor(tableName, sensorID, startTime, endTime)   # 换API getBySensorsWithTime 为 getBySensorWithTime
        DataSet.conn.close()
        return result

    def getData(self, table):
        now = datetime.datetime.now()+datetime.timedelta(days=12)
        starttime = datetime.datetime.now() + datetime.timedelta(days=-365)  # one year ago
        now = now.strftime("%Y-%m-%d %H:%M:%S")
        starttime = starttime.strftime("%Y-%m-%d %H:%M:%S")
        rs = DataSet.conn.getByTime(table, starttime, now)
        print table
        count = DataSet.conn.getRowCount(table)
        print "Total rowcount", count  # test
        if not rs or rs.errCode() != errorcode.OK:
            print 'do not find any record'
            DataSet.conn.close()
            return 0
        count = rs.size()
        print 'row count is %d', count
        rows = rs.getRows(0, count)
        time.sleep(0.5)
        return rows

    def save(self, x=0, y=0, z=0):  # sensor in method tablename= datatype
        sampledTimestamp = datetime.datetime.now()
        regionList = Region.getAll()
        for item in regionList:
            if item.getSaveStatus() is False:
                attributes = {"ParentRegion": item.getParentRegion()}
                self.dataset.insertData("Region", item.getName(), sampledTimestamp, x, y, z, attributes)
                item.setStatusTrue()
        print "Region updated"

        deviceList = Device.getAll()
        for item in deviceList:
            if item.getSaveStatus() is False:
                attributes = {"Region": item.getUpperLayer()}
                attributes.update(item.getAttributes())
                self.dataset.insertData("Device", item.getName(), sampledTimestamp, x, y, z, attributes)
                item.setStatusTrue()
        print "Device updated"

        sensorList = Sensor.getAll()
        for item in sensorList:
            if item.getSaveStatus() is False:
                attributes = {"Device": item.getUpperLayer()}
                attributes.update(item.getAttributes())
                self.dataset.insertData("Sensor", item.getName(), sampledTimestamp, x, y, z, attributes)
                item.setStatusTrue()
        print "Sensor updated"

        dataList = Data.getAll()
        for item in dataList:
            if item.getSaveStatus() is False:
                attributes = {"Sensor": item.getUpperLayer()}
                self.dataset.insertData("Data", item.getName(), sampledTimestamp, x, y, z, attributes)
                item.setStatusTrue()
        print "Data updated"

        print "All saved."

    def update(self):
        DataSet.conn.connect(DataSet.host, DataSet.port)
        flagRegion = 0
        flag = 0
        rows = self.getData("Region")
        for i in xrange(len(rows)):
            region = rows[i].getSensorID()
            regionObject = Region(region)
        regionList = Region.getAll()
        regionNameList = [name.getName() for name in regionList]

        print regionNameList

        for i in xrange(len(rows)):
            parentRegion = rows[i].getString("ParentRegion")
            print parentRegion
            regionName = rows[i].getSensorID()
            if parentRegion != "-1":
                for index, region in enumerate(regionNameList):
                    if region == parentRegion:
                        flagRegion = index
                parentRegionObject = regionList[flagRegion]

                for index, region in enumerate(regionNameList):
                    if region == regionName:
                        flag = index
                regionObject = regionList[flag]
                parentRegionObject.addRegionobject(regionObject)


        rows = self.getData("Device")
        for i in xrange(len(rows)):
            deviceName = rows[i].getSensorID()
            manufacturer = rows[i].getString("manufacturer")
            temperatureRange = rows[i].getString("temperatureRange")
            regionName = rows[i].getString("Region")
            deviceObject = Device(deviceName, manufacturer, temperatureRange)
            for index, region in enumerate(regionNameList):
                if region == regionName:
                     flagRegion = index
            region = regionList[flagRegion]
            region.addDevice(deviceObject)

        deviceList = Device.getAll()
        deviceNameList = [name.getName() for name in deviceList]

        rows = self.getData("Sensor")
        for i in xrange(len(rows)):
            sensorName = rows[i].getSensorID()
            unit = rows[i].getString("unit")
            temperatureRange = rows[i].getString("temperatureRange")
            manufacturer = rows[i].getString("manufacturer")
            deviceName = rows[i].getString("Device")
            sensorObject = Sensor(sensorName, unit, manufacturer, temperatureRange)
            for index, region in enumerate(deviceNameList):
                if region == deviceName:
                     flag = index
            device = deviceList[flag]
            device.addSensorobject(sensorObject)

        sensorList = Sensor.getAll()
        sensorNameList = [name.getName() for name in sensorList]

        rows = self.getData("Data")
        for i in xrange(len(rows)):
            dataName = rows[i].getSensorID()
            sensorName = rows[i].getString("Sensor")
            dataObject = Data(dataName)
            for index, region in enumerate(sensorNameList):
                if region == sensorName:
                    flag = index
            sensor = sensorList[flag]
            sensor.addDataobject(dataObject)

        print "Updated successfully"
        DataSet.conn.close()
