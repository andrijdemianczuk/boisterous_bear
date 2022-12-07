from Entities import KTemp
from Helpers import Publish

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

        # testing kafka connectivity - as of Dec. 7, 2022 this works okay
        # tlist = ['a', 'b', 'c']
        # kafkaProducer = Publish.connect_kafka_producer()
        # for t in tlist:
        #     Publish.publish_message(kafkaProducer, 'dev1', 'raw', t.strip())
        #
        # if kafkaProducer is not None:
        #     kafkaProducer.close()


    except Exception as ex:
        print('Exception in main method message')
        print(str(ex))
