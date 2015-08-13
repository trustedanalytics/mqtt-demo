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

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OnFeatureVectorArrived implements MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnFeatureVectorArrived.class);

    private final Function<float[], Boolean> scoringEngine;
    private final Consumer<Float> scoringStore;

    public OnFeatureVectorArrived(Function<float[], Boolean> scoringEngine,
        Consumer<Float> scoringStore) {
        this.scoringEngine = scoringEngine;
        this.scoringStore = scoringStore;
    }

    @Override public void connectionLost(Throwable throwable) {
        LOGGER.error("Connection lost. " + throwable);
    }

    @Override public void messageArrived(String topic, MqttMessage mqttMessage)
        throws Exception {

        LOGGER.debug("message: {}", mqttMessage);

        final List<Float> dataVector = getDataVector(mqttMessage);
        if (dataVector.size() < 2) {
            LOGGER.warn(
                "Bad input data format: we're looking for at least 2 array elements, but got only {}",
                dataVector.size());
            return;
        }

        float score = dataVector.get(0);
        float[] featureArray = getFeatureArray(dataVector, score);

        if (!scoringEngine.apply(featureArray)) {
            LOGGER.debug("Anomaly detected - store info in Influx");
            scoringStore.accept(score);
        } else {
            LOGGER.debug("No anomaly detected");
        }
    }

    private List<Float> getDataVector(MqttMessage mqttMessage) {
        return Arrays.stream(mqttMessage.toString().split(","))
            .map(Float::parseFloat)
            .collect(Collectors.toList());
    }

    private float[] getFeatureArray(List<Float> dataVector, float score) {
        List<Float> featureVector = dataVector.subList(1, dataVector.size());
        LOGGER.debug("score: {}", score);
        LOGGER.debug("featureVector: {}", featureVector);

        float[] featureArray = new float[featureVector.size()];
        for (int i = 0; i < featureArray.length; ++i) {
            featureArray[i] = featureVector.get(i);
        }
        return featureArray;
    }

    @Override public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        LOGGER.debug("deliveryComplete: " + iMqttDeliveryToken);
    }
}

