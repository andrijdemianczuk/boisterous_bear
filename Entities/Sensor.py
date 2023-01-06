from time import time
from typing import Dict
from uuid import uuid4
from os import stat, rename
from datetime import datetime
from Helpers import Publish
import json


# Offsets will be used for DTS line depth
def getOffsetDay():
    return {"0": 1.0, "1": 1.0, "2": 1.0, "3": 1.0, "4": 1.0, "5": 0.9,
            "6": 0.9}


def getOffsetHr():
    return {"0": 0.68, "1": 0.69, "2": 0.7, "3": 0.66, "4": 0.65, "5": 0.72, "6": 0.85, "7": 0.88,
            "8": 0.9,
            "9": 0.94,
            "10": 1.0, "11": 1.1, "12": 1.17, "13": 1.15, "14": 1.11, "15": 1.1, "16": 0.99, "17": 0.97,
            "18": 0.95,
            "19": 0.9, "20": 0.7, "21": 0.72, "22": 0.75, "23": 0.7}


def getOffsetDepth() -> object:
    return {"0": 1.00}

def getOffsetMin():
    return {"0": 0.44,
            "1": 0.43,
            "2": 0.40,
            "3": 0.41,
            "4": 0.42,
            "5": 0.40,
            "6": 0.39,
            "7": 0.38,
            "8": 0.39,
            "9": 0.39,
            "10": 0.40,
            "11": 0.42,
            "12": 0.45,
            "13": 0.51,
            "14": 0.55,
            "15": 0.60,
            "16": 0.62,
            "17": 0.69,
            "18": 0.73,
            "19": 0.77,
            "20": 0.81,
            "21": 0.82,
            "22": 0.84,
            "23": 0.85,
            "24": 0.87,
            "25": 0.89,
            "26": 0.92,
            "27": 0.90,
            "28": 1.0,
            "29": 1.05,
            "30": 1.1,
            "31": 1.07,
            "32": 1.03,
            "33": 1.01,
            "34": 1.0,
            "35": 0.95,
            "36": 0.92,
            "37": 0.90,
            "38": 0.88,
            "39": 0.85,
            "40": 0.79,
            "41": 0.77,
            "42": 0.73,
            "43": 0.68,
            "44": 0.66,
            "45": 0.61,
            "46": 0.59,
            "47": 0.58,
            "48": 0.55,
            "49": 0.54,
            "50": 0.53,
            "51": 0.52,
            "52": 0.50,
            "53": 0.48,
            "54": 0.47,
            "55": 0.43,
            "56": 0.40,
            "57": 0.41,
            "58": 0.43,
            "59": 0.45
            }


class Sensor(object):

    def __init__(self, writeLocation="temp/", srcLocation="IoT_Sensor_Template/") -> None:
        self.srcName = None
        self.newFilePath = None
        self.filePath = None
        self.fileIsEmpty = None
        self.offsetDay = getOffsetDay()
        self.offsetHr = getOffsetHr()
        self.srcLocation = srcLocation
        self.writeLocation = writeLocation
        self.guid = str(uuid4())
        self.epoch_time = int(time())
        # self.offset = self.getOffset(self.epoch_time)
        self.fileRolloverLimitB = 10485760  # 10Mb per file

    def openFile(self, fp: str) -> object:
        # Params
        self.fileIsEmpty = False

        # Open the file if exists and append, otherwise create a new one at the specified file path
        f1 = open(fp, "a")

        # Set a flag to check if the file is empty.
        if stat(fp).st_size == 0:
            self.fileIsEmpty = True

        return f1

    def getOffset(self, epoch_time) -> float:
        dayOfWeek = datetime.today().weekday()  # used to modify for weekends / non-business days
        d = datetime.fromtimestamp(epoch_time)  # used for random offset by hour-of-day

        return self.offsetHr[str(d.hour)] * self.offsetDay[str(dayOfWeek)]

    def rollover(self):
        # Rollover the file if it's greater or equal to the limit.
        if stat(self.filePath).st_size >= self.fileRolloverLimitB:
            rename(self.filePath, self.newFilePath)

    def run(self):
        # Open the file write location
        f1 = self.openFile(self.filePath)

        # Define the source of the template file
        srcFile = self.srcLocation + self.srcName

        # Process the file output
        self.processObjFile(srcFile, f1)  # This is each derived class - It's essentially an abstract factory

        # Check and apply rollover
        self.rollover()

    def processObjFile(self, srcFile, f1):
        pass


class KSensor(object):

    def __init__(self, topic: str = "default", bootstrap_servers: [] = 'localhost') -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.offsetDay = getOffsetDay()
        self.offsetHr = getOffsetHr()
        self.offsetMin = getOffsetMin()
        # not sure if we still need UUID - we're not doing a file rollover, but might still be useful.
        self.guid = str(uuid4())

    def getOffset(self, epoch_time) -> int:
        # used for random offset by minute-of-hour
        d = datetime.fromtimestamp(epoch_time)
        return self.offsetMin[str(d.minute)]

    def run(self):
        self.generateStreamSource()

    def generateStreamSource(self):
        pass
