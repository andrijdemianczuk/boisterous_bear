import csv
import random
from Entities import Sensor


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


    def __init__(self, topic: str = "default", bootstrap_servers: [] = 'localhost') -> None:
        super().__init__(topic, bootstrap_servers)
        self.srcName = "Temp.csv"
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def generateStreamSource(self):
        # This will be the main function to generate and return the JSON payload
        pass
