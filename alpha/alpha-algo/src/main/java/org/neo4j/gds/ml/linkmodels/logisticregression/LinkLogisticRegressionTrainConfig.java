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
package org.neo4j.gds.ml.linkmodels.logisticregression;

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
public interface LinkLogisticRegressionTrainConfig extends FeaturePropertiesConfig, TrainingConfig {

    @Configuration.Parameter
    List<String> featureProperties();

    @Value.Default
    default double penalty() {
        return 0.0;
    }

    @Value.Default
    default String linkFeatureCombiner() {
        return LinkFeatureCombiners.L2.name();
    }

    @Configuration.CollectKeys
    @Value.Auxiliary
    @Value.Default
    @Value.Parameter(false)
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    static LinkLogisticRegressionTrainConfig of(
        List<String> featureProperties,
        int defaultConcurrency,
        Map<String, Object> params
    ) {
        var cypherMapWrapper = CypherMapWrapper.create(params);
        if (!cypherMapWrapper.containsKey(CONCURRENCY_KEY)) {
            cypherMapWrapper = cypherMapWrapper.withNumber(CONCURRENCY_KEY, defaultConcurrency);
        }
        var config = new LinkLogisticRegressionTrainConfigImpl(
            featureProperties,
            cypherMapWrapper
        );
        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());
        return config;
    }
}
