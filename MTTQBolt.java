/*
 Copyright 1.0 Soguy Mak-Karé Gueye

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package /* to set */;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.json.simple.JSONObject;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttTopic;


public class MQTTBolt implements MqttCallback, IRichBolt {
        MqttClient client;
        OutputCollector _collector;

        String _broker_url;
        String _client_id;
        String _topic;
	int _qos;

	MqttTopic topic;


        public MQTTBolt (String broker_url, String clientId, String topic, int qos) {
                _broker_url = broker_url;
                _client_id = clientId;
                _topic = topic;
		_qos = qos;
        }



	/**
	 * messageArrived
	 * This callback is invoked when a message is received on a subscribed topic.
	 */
        public void messageArrived(String topic, MqttMessage message)throws Exception {
                // TODO
        }

	

	/**
	 * connectionLost
	 * This callback is invoked upon losing the MQTT connection.
	 */
        public void connectionLost(Throwable cause) {
		 client.disconnect();
		 client.connect();
                 client.setCallback(this);
        }


	/**
	 * deliveryComplete
	 * This callback is invoked when a message published by this client is successfully received by the broker.
	 * @throws MqttException 
	 */
        public void deliveryComplete(IMqttDeliveryToken token) { 
                // TODO
        }




	public void handle (Tuple tuple) {
		Integer house_id       = (Integer)tuple.getValueByField("house_id");
		Integer household_id   = (Integer)tuple.getValueByField("household_id");
		Integer plug_id        = (Integer)tuple.getValueByField("plug_id");
		Integer timeStamp      = (Integer)tuple.getValueByField("timeStamp");
		Double  predicted_load = (Double)tuple.getValueByField("predicted_load");


		JSONObject obj = new JSONObject();
		obj.put("ts",timeStamp);
		obj.put("house_id",house_id);
		obj.put("household_id",household_id);
		obj.put("plug_id",plug_id);
		obj.put("predicted_load",predicted_load);

		MqttMessage message = null;
		try {
			message = new MqttMessage(obj.toJSONString().getBytes("UTF-8"));				
			message.setQos(qos);
			message.setRetained(false);		
			MqttDeliveryToken token = null;
			if(topic == null ) topic = myClient.getTopic(_topic);
			token = topic.publish(message);
			token.waitForCompletion();

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (MqttPersistenceException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}
        }





	@Override
    	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        	_collector = collector;
                try {
                        client = new MqttClient(_broker_url, _client_id, new MqttDefaultFilePersistence("/tmp"));
                        client.connect();
                        client.setCallback(this);
                } catch (MqttException e) {
                        e.printStackTrace();
                }
    	}


        public void execute(Tuple tuple)  {
		handle (tuple) ;
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	//TODO
        }

        public Map<String, Object> getComponentConfiguration() {
                return null;
        }

}
