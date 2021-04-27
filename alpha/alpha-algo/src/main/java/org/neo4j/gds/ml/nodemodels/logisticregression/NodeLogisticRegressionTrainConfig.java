/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.immutables.value.Value;
import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
// This class is currently used internally in NodeClassification and is not
// a procedure-level configuration. it is derived from a NodeClassificationTrainConfig
public interface NodeLogisticRegressionTrainConfig extends FeaturePropertiesConfig, TrainingConfig {

    @Configuration.Parameter
    List<String> featureProperties();

    @Configuration.Parameter
    String targetProperty();

    double penalty();

    @Configuration.CollectKeys
    @Value.Auxiliary
    @Value.Default
    @Value.Parameter(false)
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    @Configuration.Ignore
    @Value.Derived
    @Value.Auxiliary
    default Map<String, Object> toMap() {
        return Map.of(
            "featureProperties", featureProperties(),
            "penalty", penalty(),
            "targetProperty", targetProperty(),
            "batchSize", batchSize(),
            "minEpochs", minEpochs(),
            "patience" , patience(),
            "maxEpochs", maxEpochs(),
            "tolerance" , tolerance(),
            "sharedUpdater", sharedUpdater(),
            "concurrency", concurrency()
        );
    }

    static NodeLogisticRegressionTrainConfig of(
        List<String> featureProperties,
        String targetProperty,
        int defaultConcurrency,
        Map<String, Object> params
    ) {
        var cypherMapWrapper = CypherMapWrapper.create(params);
        if (!cypherMapWrapper.containsKey(CONCURRENCY_KEY)) {
            cypherMapWrapper = cypherMapWrapper.withNumber(CONCURRENCY_KEY, defaultConcurrency);
        }
        var config = new NodeLogisticRegressionTrainConfigImpl(
            featureProperties,
            targetProperty,
            cypherMapWrapper
        );
        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());
        return config;
    }

}
