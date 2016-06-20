from View import View, SensorIDView, DataView, DeviceView, SensorView
from Data import Data
from Sensor import Sensor
from Device import Device
from Region import Region
from DataSet import DataSet

import datetime
import time
import random
from org.cesl.sensordb.client.connection import Connection
from org.cesl.sensordb.client import errorcode

def testDevice():
        device = DeviceView()
        device.update()
        device_list = device.deviceView("Wang")
        print device_list

def testGetRootRegion():
    view = View()
    view.update()
    tmp = view.getAllRegion()
    rootRegionList = []
    for region in tmp:
        if region.getParentRegion() == -1:
            rootRegionList.append(region)
    # global rootRegion
    # rootRegion = rootRegionList
    return rootRegionList

def printResultSet(rs):
    if not rs or rs.errCode() != errorcode.OK:
        print 'do not find any record'
        return

    print 'row count is %d' % rs.size()

    printRowCount = 10
    if rs.size() < 10:
        printRowCount = rs.size()
    rowkeys = rs.getRowKeys(0, printRowCount)
    if rowkeys:
        for i in xrange(len(rowkeys)):
            print 'row key is %s' % rowkeys[i];

    rows = rs.getRows(0, printRowCount)
    if rows:
        for i in xrange(len(rows)):
            loc = rows[i].getSampledLocation()
            print '%s_%d_%f_%f_%f_%s' % (rows[i].getSensorID(), rows[i].getSampledTimestamp(), loc[0], loc[1], loc[2],
                                         rows[i].getString("DataType"))
            # print type(rows[i].getString("ParentRegion"))
            print rows[i].getDouble('co')

def testGetByTime():
    print 'testGetByTime'
    conn = Connection()
    conn.connect("127.0.0.1", 6677)
    rs = conn.getByTime("RawData", '2013-01-01 00:00:00', '2017-12-01 00:00:00')
    try:
        printResultSet(rs)
    finally:
        conn.close()

def getRootRegion():
    view = View()
    view.update()
    rootRegionList = []
    for region in view.getAllRegion():
        if region.getParentRegion() == -1:
            rootRegionList.append(region)
    global rootRegion
    rootRegion = rootRegionList
    return rootRegionList

def testGetTree():
    p1 = 0
    p2 = 0
    p3 = 0
    p4 = 0
    p5 = 0
    dataList = []
    for root in getRootRegion():
        tmpdict = {}
        p1 += 1
        id = '%d' % p1
        pId = "0"
        name = root.getName()
        tmpdict["id"] = id
        tmpdict["pId"] = pId
        tmpdict["name"] = name
        dataList.append(tmpdict)
        for sub in root.getSubRegions():
            tmpdict = {}
            p2 += 1
            id = ('%d' % p1) + ('%d' % p2)
            pId = ('%d' % p1)
            name = sub.getName()
            tmpdict["id"] = id
            tmpdict["pId"] = pId
            tmpdict["name"] = name
            dataList.append(tmpdict)
            for device in sub.getDevicesUnderRegion():
                tmpdict = {}
                p3 += 1
                id = ('%d' % p1) + ('%d' % p2) + ('%d' % p3)
                pId = ('%d' % p1) + ('%d' % p2)
                name = device.getName()
                tmpdict["id"] = id
                tmpdict["pId"] = pId
                tmpdict["name"] = name
                dataList.append(tmpdict)
                for sensor in device.getSensors():
                    tmpdict = {}
                    p4 += 1
                    id = ('%d' % p1) + ('%d' % p2) + ('%d' % p3) + ('%d' % p4)
                    pId = ('%d' % p1) + ('%d' % p2) + ('%d' % p3)
                    name = sensor.getName()
                    tmpdict["id"] = id
                    tmpdict["pId"] = pId
                    tmpdict["name"] = name
                    dataList.append(tmpdict)
                    for data in sensor.getDatatypeUndersensor():
                        tmpdict = {}
                        p5 += 1
                        id = ('%d' % p1) + ('%d' % p2) + ('%d' % p3) + ('%d' % p4) + ('%d' % p5)
                        pId = ('%d' % p1) + ('%d' % p2) + ('%d' % p3) + ('%d' % p4)
                        name = data
                        tmpdict["id"] = id
                        tmpdict["pId"] = pId
                        tmpdict["name"] = name
                        dataList.append(tmpdict)
    print dataList

def testin():
    Lab1 = Region("Lab1-1")
    Lab2 = Region("Lab2-2")
    Lab3 = Region("11-304")
    Lab4 = Region("11-305")

    CO = Data("CO")
    CO2 = Data("CO2")
    Gas = Sensor("004", "table_test_2")
    Gas2 = Sensor("005", "table_test_2")
    Car = Device("computer")
    Truck = Device("light")
    Car.addHierarchy(Gas, CO)
    Truck.addHierarchy(Gas, CO2)
    Lab1.addDevice(Car)
    Lab2.addDevice(Truck)
    Lab3.addDevice(Car)
    Lab4.addDevice(Truck)

    Test = SensorView()
    Test.createTable("table_test_2")
    # Test2 = DeviceView()
    Wang = Region("Wu")
    Wang.addRegionobject(Lab1)
    Wang.addRegionobject(Lab2)

    Wang = Region("Wu")
    Wang.addRegionobject(Lab3)
    Wang.addRegionobject(Lab4)
    # d = datetime.datetime.now()   # need attention. check the three possible format here
    # print d
    for item in range(10):
        d = datetime.datetime.now()
        print d
        # value1 = random.randint(1, 100)
        # value2 = random.randint(1, 80)
        value3 = random.randint(1, 60)
        value4 = random.randint(1, 70)

        # Test.saveData(Lab1, Car, Gas, CO, datetime.datetime.now(), value1, 2, 3, 5)
        # Test.saveData(Lab2, Truck, Gas2, CO2, datetime.datetime.now(), value2, 2, 3, 5)
        Test.saveData(Lab3, Car, Gas, CO, datetime.datetime.now(), value3, 2, 3, 5)
        Test.saveData(Lab4, Truck, Gas2, CO2, datetime.datetime.now(), value4, 2, 3, 5)

def testInput():
    now = datetime.datetime.now() + datetime.timedelta(days=12)
    starttime = datetime.datetime.now() + datetime.timedelta(days=-365)  # one year ago
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    starttime = starttime.strftime("%Y-%m-%d %H:%M:%S")
    print now, starttime
    rs = DataSet.conn.getByTime("table_test_2", starttime, now)
    print 'row count is %d' % rs.size()
    printRowCount = rs.size()
    rows = rs.getRows(0, printRowCount)

def testInDist():
    Wang = Region("Wang")
    Lab1 = Region("Lab1")
    Lab2 = Region("Lab2")
    Computer = Device("Computer", "lenovo", "-2 - 80C")
    Light = Device("Light", "asus", "-3 - 80C")
    Sensor1 = Sensor("001", "vol", "Ti", "-1 - 80C")
    Sensor2 = Sensor("002", "Lm", "Dell", "-4 - 80C")
    CO = Data("CO")
    White = Data("WhiteLight")

    Wang.addRegionobject(Lab1)
    Wang.addRegionobject(Lab2)

    Lab1.addDevice(Computer)
    Lab2.addDevice(Light)

    Computer.addHierarchy(Sensor1, CO)
    Light.addHierarchy(Sensor2, White)

    Test = View()
    Test.createTable("Region")
    Test.createTable("Device")
    Test.createTable("Sensor")
    Test.createTable("Data")
    Test.save()

def testRawDataIn():
    Test = View()
    # Test.createTable("RawData")
    while(1):
        d = datetime.datetime.now()
        print d
        value3 = random.randint(1, 60)
        value4 = random.randint(1, 70)
        time.sleep(3)
        Test.putRawData("001", "CO", d,  value3)
        Test.putRawData("002", "WhiteLight", d, value4)


if __name__ == "__main__":
    testRawDataIn()
    # testInDist()
    # for item in Test.getAllRegion():
    #     print item.getParentRegion()
    # name = [item.getName() for item in Region.getAll()]
    # sensor = SensorView()
    # sensor.sensorView("Wang")
    # sensor.sensorView("Lab1")
    # print name
    # data = DataView()
    # print data.dataView("001", "CO",'2013-01-01 00:00:00', '2017-12-01 00:00:00')
    # testGetByTime()