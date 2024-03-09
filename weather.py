import json
import paho.mqtt.client as mqtt #import the client1
import weatherhat
from weatherhat.history import WindSpeedHistory
import time
from datetime import datetime




class Sample:
   def __init__(self, sample_timestamp, 
                      station,
                      temperature, 
                      humidity,
                      dew_point,
                      pressure,
                      wind_speed,
                      wind_direction,
                      rain,
                      wind_avg_mph,
                      wind_max_mph):
      
      self.sample_timestamp = sample_timestamp
      self.station = station
      self.temperature = temperature
      self.humidity = humidity
      self.dew_point = dew_point
      self.pressure = pressure
      self.wind_speed = wind_speed
      self.wind_direction = wind_direction
      self.rain = rain
      self.wind_avg_mph = wind_avg_mph
      self.wind_max_mph = wind_max_mph

   def to_json(self):
       return json.dumps(self, default=lambda o: o.__dict__)


def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    


def on_log(client, userdata, level, buf):
    print("log: ",buf)    


broker_address="192.168.1.119"

firstSample = True
sensor = weatherhat.WeatherHAT()

wind_speed_history = WindSpeedHistory()


print("creating new instance")
client = mqtt.Client("P1") #create new instance

client.on_message=on_message #attach function to callback
client.on_log=on_log

print("connecting to broker")
client.connect(broker_address) #connect to broker


client.loop_start() #start the loop

while True:
    
    # skip first sample; always erroneous 
    
    #sensor.temperature_offset = 0.0
    

    wind_speed_history.append(sensor.wind_speed)
    print(f"Average wind speed: {wind_speed_history.average_mph(600)}mph")
    print(f"Wind gust: {wind_speed_history.gust_mph(600)}mph")
        

    now = datetime.now()
    
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    print("date & time = ", ts )    


    cTemp = sensor.temperature + 5.0

    sensor.update(interval=15.0)
    
    sample = Sample( ts,
                     "WEATHER-1",
                     '%.2f' % cTemp,
                     '%.2f' % sensor.relative_humidity,
                     '%.2f' % sensor.dewpoint,
                     '%.2f' % sensor.pressure,
                     '%.2f' % sensor.wind_speed,
                     sensor.wind_direction,
                     '%.2f' % sensor.rain,
                     '%.2f' % wind_speed_history.average_mph(600),
                     '%.2f' % wind_speed_history.gust_mph(600) )

        
        
    if not firstSample:
        print("Publishing message to topic","test")
        client.publish("test",sample.to_json(), 2, True)
 
    time.sleep(15) # wait
    firstSample = False
