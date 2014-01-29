twitterstorm-mqttspout
======================

MQTT spout

The Constructor takes as parameters:
  - The URL of the MQTT broker:        String broker_url
  - The client ID:                     String clientId
  - The topics to subscribe:           String[] topic
  - For each topic the associated qos: int [] qos
  
  
In the method open of the spout:
  - We create the MqttClient. We specify the directory /tmp for the persistent data store.
  - We connect the client
  - We subscribe to the topics
  

We put the received messages from the MQTT broker 
  - in a list: LinkedList<String> messages
  - the messages are removed from the list and transmitted in the topology
     - at each call of NextTuple


