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

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.TrainingConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Configuration
// This class is currently used internally in NodeClassification and is not
// a procedure-level configuration. it is derived from a NodeClassificationTrainConfig
public interface NodeLogisticRegressionTrainConfig extends FeaturePropertiesConfig, TrainingConfig {

    @Configuration.Parameter
    List<String> featureProperties();

    @Configuration.Parameter
    String targetProperty();

    double penalty();

    @Configuration.CollectKeys
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    @Configuration.ToMap
    Map<String, Object> toMap();

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

    static NodeLogisticRegressionTrainConfig empty() {
        return new NodeLogisticRegressionTrainConfigImpl(List.of(), "", CypherMapWrapper.create(Map.of("penalty", 1)));
    }

}
