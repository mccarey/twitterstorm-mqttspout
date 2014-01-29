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

/*package to set ;*/

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
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.json.simple.JSONObject;


public class MQTTSpout implements MqttCallback, IRichSpout {

        public MqttClient client;
        public SpoutOutputCollector _collector;

        public LinkedList<String> messages;

        public String _broker_url;
        public String _client_id;

        public String [] _topics = null;
    	public int[] _qos = null;


        public MQTTSpout(String broker_url, String clientId, String[] topic, int [] qos) {
                _broker_url = broker_url;
                _client_id = clientId;
                _topic = topic;
                _qos = qos;
                messages = new LinkedList<String>();
        }


        public void messageArrived(String topic, MqttMessage message)throws Exception {
                messages.add(message.toString());
        }

	

        public void connectionLost(Throwable cause) {
		 client.disconnect();
		 client.connect();
                 client.setCallback(this);
                 client.subscribe(_topic);
        }

        public void deliveryComplete(IMqttDeliveryToken token) {
		//TODO 
        }

	
	private void subscribe( ) throws Exception {
		if(_topics!= null && 0 < _topics.length) {
			if(_qos != null && _qos.length == _topics.length){
				myClient.subscribe(_topics, _qos) ;
			} else {
				myClient.subscribe(_topics) ;
			}		
		}
		//TODO 	
	}

	private void unsubscribe( ) throws Exception {
	 	if(_topics!= null && 0 < _topics.length) {
			myClient.unsubscribe(_topics) ;
		}
		//TODO 
	}


        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
                _collector = collector;
                try {			
                        client = new MqttClient(_broker_url, _client_id, new MqttDefaultFilePersistence("/tmp"));
                        client.connect();
                        client.setCallback(this);
                        subscribe( ); // client.subscribe(_topic);

                } catch (MqttException e) {
			//TODO		               
                }
        }

        public void close() {
		//TODO	
        }

        public void activate() {
		//TODO		    

        }

        public void deactivate() {
		//TODO  
        }


        public void nextTuple() {
                while (!messages.isEmpty()) {	
			_collector.emit(new Values(messages.poll()));
                }
        }

        public void ack(Object msgId) {
		//TODO		    
        }

        public void fail(Object msgId) {
		//TODO		    
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	declarer.declare(new Fields("message"));
        }

        public Map<String, Object> getComponentConfiguration() {
                return null;
        }

}
