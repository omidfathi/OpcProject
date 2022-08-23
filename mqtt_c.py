import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe


class Mqtt:

    def mqtt_connection_initial(self, mqttclientid, url, port):
        self.client = mqtt.Client(mqttclientid)
        self.url = url
        self.port = port

    def mqtt_connection(self):
        self.client.connect(host=self.url, port=self.port)
        self.client.loop_start()

    def on_connect(self, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
            self.client.loop_stop()

    def on_subscribe(self, userdata, mid, granted_qoss):
        print("Subscribed", userdata)

    def on_publish(self, userdata, mid):
        print("In on_pub callback mid= "+ str(mid))

    def on_message(self, userdata, message):
        print("New message received: ", str(message.payload.decode("utf_8")),"Topic: %s",message.topic,
              "Retained: %s",message.retain)


    def mqtt_pub(self, topic, payload, qos):
        self.client.publish(topic=topic, payload=payload,qos=qos)
        print(f"Message{payload},was published to {topic} topic")

    def mqtt_sub(self, topic, qos):
        self.client.subscribe(topic=topic, qos=qos)
        # subscribe._on_message_simple(self.client)

