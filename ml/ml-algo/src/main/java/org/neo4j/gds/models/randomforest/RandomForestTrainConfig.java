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
package org.neo4j.gds.models.randomforest;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfig;
import org.neo4j.gds.models.TrainerConfig;
import org.neo4j.gds.models.TrainingMethod;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

@Configuration
public interface RandomForestTrainConfig extends DecisionTreeTrainConfig, TrainerConfig {

    @Configuration.DoubleRange(min = 0, max = 1, minInclusive = false)
    // Defaults to 1.0/sqrt(featureDimension) if not set explicitly.
    Optional<Double> maxFeaturesRatio();

    @Configuration.Ignore
    default double maxFeaturesRatio(int featureDimension) {
        return maxFeaturesRatio().orElse(1.0D / Math.sqrt(featureDimension));
    }

    @Configuration.DoubleRange(min = 0, max = 1)
    // A value of 0 means "sampling off": Do not sample, rather use all training examples given.
    default double numberOfSamplesRatio() {
        return 1;
    }

    @Configuration.IntegerRange(min = 1)
    default int numberOfDecisionTrees() {
        return 100;
    }

    @Override
    @Configuration.Ignore
    default String methodName() {
        return TrainingMethod.RandomForest.name();
    }

    static RandomForestTrainConfig of(Map<String, Object> params) {
        var cypherMapWrapper = CypherMapWrapper.create(params);

        var config = new RandomForestTrainConfigImpl(cypherMapWrapper);

        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());
        return config;
    }

    @Configuration.CollectKeys
    Collection<String> configKeys();

    @Configuration.ToMap
    Map<String, Object> toMap();
}
