import paho.mqtt.client as mqtt
import json

from myconnection_ssh import connect_to_mysql

config = {
    "host": "127.0.0.1",
    "user": "greg",
    "password": "Bl33dBlu3",
    "database": "weather",
}

broker_address="192.168.1.119"


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("test")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    
    payload = msg.payload.decode("utf-8")
    sample = json.loads(payload)
    
    print("temp:", sample['temperature'])
    
    
    val = ( sample['sample_timestamp'],     
            sample['station'],
            sample['temperature'],
            sample['humidity'],
            sample['dew_point'],
            sample['pressure'],
            sample['wind_speed'],
            sample['wind_direction'],
            sample['rain'],
            sample['wind_avg_mph'],
            sample['wind_max_mph'])
        
    connect_to_mysql(config, val, attempts=3)

    

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()