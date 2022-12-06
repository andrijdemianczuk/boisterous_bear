from Entities import KTemp

if __name__ == '__main__':
    # sHumidity.Humidity("temp/", "IoT_Sensor_Template/").run()
    # sProx.Prox("temp/", "IoT_Sensor_Template/").run()
    # sTemp.Temp("temp/", "IoT_Sensor_Template/").run()
    # sLight.Light("temp/", "IoT_Sensor_Template/").run()
    # sPressure.Pressure("temp/", "IoT_Sensor_Template/").run()

    try:
        # sHumidity.KHumidity(topic="dev1", bootstrap_servers=['35.86.112.176:9092']).run() #topic name should be
        # something like 'building_iot_humidity'
        KTemp.KTemp(topic="dev1", bootstrap_servers=['35.86.112.176:9092']).run()

    except Exception as ex:
        print('Exception in main method message')
        print(str(ex))
