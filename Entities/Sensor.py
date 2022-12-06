from time import time
from uuid import uuid4
from os import stat, rename
from datetime import datetime


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
        self.offset = self.getOffset(self.epoch_time)
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
        self.guid = str(
            uuid4())  # not sure if we still need this - we're not doing a file rollover, but might still be useful.
        self.epoch_time = int(time())
        self.offset = self.getOffset(self.epoch_time)

    def getOffset(self, epoch_time) -> float:
        dayOfWeek = datetime.today().weekday()  # used to modify for weekends / non-business days
        d = datetime.fromtimestamp(epoch_time)  # used for random offset by hour-of-day

        return self.offsetHr[str(d.hour)] * self.offsetDay[str(dayOfWeek)]

    def run(self):
        print(f"{self.offset} {self.guid}")
