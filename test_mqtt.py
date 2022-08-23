from mqtt_c import Mqtt
import paho.mqtt.client as mqtt
mqtt_ = Mqtt()

def main():


    # mqtt_.mqtt_pub(topic="ready_to_Recieve_opc_topic", payload="", qos=0)
    # mqtt_.mqtt_sub(topic="opc_url", qos=0)
    def on_connect(self, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
            self.client.loop_stop()

    def on_message(self, userdata, message):
        print("New message received: ", str(message.payload.decode("utf_8")),"Topic: %s",message.topic,
              "Retained: %s",message.retain)

    mqtt__ = mqtt_.mqtt_connection_initial("opc_Socket", "192.168.1.51", 1883)
    mqtt_.mqtt_connection()

    mqtt.Client.on_message = on_message
    mqtt.Client.on_connect = on_connect
    mqtt.Client.loop_forever(mqtt__)
if __name__ == '__main__':
    main()
