import csv
import random
from Entities import Sensor
import json
import time
from Helpers import Publish


class Temp(Sensor.Sensor):

    def __init__(self, writeLocation: str, srcLocation: str) -> None:
        super().__init__(writeLocation, srcLocation)
        self.srcName = "Temp.csv"
        self.filePath = self.writeLocation + "Temp_data.csv"
        self.newFilePath = self.writeLocation + "Temp_data_" + self.guid + ".csv"

    def processObjFile(self, srcFile, f1):
        # Open the source (template) file, only write the header if the file is new
        with open(srcFile) as f:
            reader = csv.reader(f)
            # If the file is already established, skip writing the header
            if not self.fileIsEmpty:
                next(reader)

            # Write each row with replacements
            for row in reader:
                if row[2] == "0":
                    row[2] = self.epoch_time
                if row[1] == "0":  # Change the value range based on location
                    if 1 <= int(row[3]) <= 130:  # Seattle
                        row[1] = round(random.randint(291, 297) * 0.9995, 4)
                    elif 131 <= int(row[3]) <= 420:  # Portland
                        row[1] = round(random.randint(291, 296) * 0.999, 4)
                    elif 421 <= int(row[3]) <= 835:  # San Francisco
                        row[1] = round(random.randint(291, 297) * 1.02, 4)
                    elif 836 <= int(row[3]) <= 880:  # Helena
                        row[1] = round(random.randint(291, 297) * 1.08, 4)
                    else:  # Boise
                        row[1] = round(random.randint(292, 297) * 1.005, 4)
                # rowStr = str(row)
                f1.write(
                    str(row).translate(
                        {ord(i): None for i in '[]\''}))  # Remove the unwanted characters '[', ']' and '''
                f1.write("\r\n")
        f1.close()


class KTemp(Sensor.KSensor):

    def __init__(self, topic: str = "default", bootstrap_servers: [] = 'localhost', deg_min: int = 15,
                 deg_max: int = 220, seg_count: int = 1, well_count: int = 1) -> None:
        super().__init__(topic, bootstrap_servers)
        self.srcName = "Temp.csv"
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.deg_min = deg_min
        self.deg_max = deg_max
        self.seg_count = seg_count
        self.well_count = well_count

    def generateStreamSource(self):

        for w in range(0, self.well_count):
            # Test data to be written
            dictionary = {
                "timestamp": time.time(),
                "well": "01-0" + str(w) + "P",
                "coordinates": "51.048615,-114.070847"
            }

            for i in range(0,1700):
                k = ("seg_" + str(i))
                if i < 20:
                    v = 40
                elif i < 100:
                    v = 15
                elif i < 200:
                    v = 60
                elif i < 300:
                    v = 90
                elif i < 400:
                    v = 95
                elif i < 500:
                    v = 100
                elif i < 600:
                    v = 105
                elif i < 700:
                    v = 100
                elif i < 800:
                    v = 95
                elif i < 900:
                    v = 190
                elif i < 1000:
                    v = 160
                elif i < 1100:
                    v = 120
                elif i < 1200:
                    v = 100
                elif i < 1300:
                    v = 60
                elif i < 1400:
                    v = 40
                elif i < 1500:
                    v = 40
                elif i < 1600:
                    v = 50
                else:
                    v = 55
                dictionary[k] = v

            # Serializing json
            json_object = json.dumps(dictionary)
            print(json_object)  # Debug only

            kafkaProducer = Publish.connect_kafka_producer(bootstrap_servers=self.bootstrap_servers)
            Publish.publish_message(kafkaProducer, self.topic, 'raw', json_object)

            if kafkaProducer is not None:
                kafkaProducer.close()

            time.sleep(1)

        # testing kafka connectivity - as of Dec. 7, 2022 this works okay
        # tlist = ['a', 'b', 'c']
        # kafkaProducer = Publish.connect_kafka_producer(bootstrap_servers=self.bootstrap_servers)
        # for t in tlist:
        #     Publish.publish_message(kafkaProducer, self.topic, 'raw', t.strip())

        # if kafkaProducer is not None:
        #     kafkaProducer.close()
