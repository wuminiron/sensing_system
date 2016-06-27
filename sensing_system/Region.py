#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Region(object):
    checkRegion = []

    def __init__(self, name):
        self.name = name  # 如何规定最大字节数
        self.parentName = "-1"  # -1为device 即根节点
        self.saveStatus = False
        self.devices = []  # connect to the data hierarchy
        self.regions = []  # point to the belonged region object
        self.top = None
        self.right = None
        self.bottom = None
        self.left = None
        Region.checkRegion.append(self)    # in convenient of the add of class

    def addRegionobject(self, regionInput):
        if regionInput not in self.regions:
            self.regions.append(regionInput)
            regionInput.parentName = self.name
        else:
            print "Existed region in {0}".format(self.name)

    def addDevice(self, device):   # double linkage
        tmp = None
        for item in self.devices:
            if device.getName() == item.getName():
                print "Existed device in {0}.".format(self.name)
                tmp = -1
        if tmp is None:
            self.devices.append(device)
            device.addRegion(self.name)


    def getName(self):
        return self.name

    def getParentRegion(self):
        return self.parentName

    @classmethod
    def getAll(cls):
        return cls.checkRegion

    def getSubRegions(self):
        return self.regions

    def getDevicesUnderRegion(self):  # adjust whether the region has child node
        if self.devices:
            return self.devices
        else:
            reanswer = []
            for region in self.regions:  # traverse
                reanswer.extend(region.devices)
            return reanswer

    def getSaveStatus(self):
        return self.saveStatus

    def getPosition(self):
        ret = []
        ret.append(self.top)
        ret.append(self.right)
        ret.append(self.bottom)
        ret.append(self.left)
        return ret

    def setStatusTrue(self):
        self.saveStatus = True

    def setPosition(self, top=None, right=None, bottom=None, left=None):
        self.top = top
        self.right = right
        self.bottom = bottom
        self.left = left
