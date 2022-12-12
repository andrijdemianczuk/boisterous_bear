from Entities import KTemp
from Helpers import Publish

if __name__ == '__main__':
    # sHumidity.Humidity("temp/", "IoT_Sensor_Template/").run()
    # sProx.Prox("temp/", "IoT_Sensor_Template/").run()
    # sTemp.Temp("temp/", "IoT_Sensor_Template/").run()
    # sLight.Light("temp/", "IoT_Sensor_Template/").run()
    # sPressure.Pressure("temp/", "IoT_Sensor_Template/").run()

    try:
        KTemp.KTemp(topic="dev1", bootstrap_servers=['35.86.112.176:9092'], deg_min=15, deg_max=200,
                    seg_count=1700).run()

    except Exception as ex:
        print('Exception in main method message')
        print(str(ex))
