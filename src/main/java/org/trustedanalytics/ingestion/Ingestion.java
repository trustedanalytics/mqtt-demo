/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.ingestion;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ingestion {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion.class);

    private final OnFeatureVectorArrived onFeatureVectorArrived;
    private final MqttProperties mqttProperties;

    private IMqttClient client;

    public Ingestion(MqttProperties mqttProperties, OnFeatureVectorArrived onFeatureVectorArrived) {
        this.onFeatureVectorArrived = onFeatureVectorArrived;
        this.mqttProperties = mqttProperties;
    }

    public void init() throws MqttException {
        try {
            String url = mqttProperties.getHostname() + ":" + mqttProperties.getPort();
            LOGGER.info("Opening MQTT connection: '{}'", url);

            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setUserName(mqttProperties.getUsername());
            connectOptions.setPassword(mqttProperties.getPassword().toCharArray());

            client = new MqttClient(url, mqttProperties.getTopic(), new MemoryPersistence());
            client.setCallback(onFeatureVectorArrived);
            client.connect(connectOptions);
            client.subscribe(mqttProperties.getTopic());

        } catch (MqttException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    public void destroy() {
        if (client != null) {
            try {
                LOGGER.debug("Closing MQTT connection.");
                client.disconnect();
            } catch (MqttException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
