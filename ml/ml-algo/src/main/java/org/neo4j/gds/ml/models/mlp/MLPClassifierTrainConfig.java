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
package org.neo4j.gds.ml.models.mlp;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.gradientdescent.GradientDescentConfig;
import org.neo4j.gds.ml.models.PenaltyConfig;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Configuration
public interface MLPClassifierTrainConfig extends GradientDescentConfig, PenaltyConfig, TrainerConfig {
    MLPClassifierTrainConfig DEFAULT = of(Map.of());

    @Value.Default
    default List<Integer> hiddenLayerSizes() {
        return List.of(100);
    }

    @Configuration.DoubleRange(min = 0.0)
    default double focusWeight() {
        return 0;
    }

    @Configuration.ToMap
    Map<String, Object> toMap();

    @Configuration.CollectKeys
    Collection<String> configKeys();

    static MLPClassifierTrainConfig of(Map<String, Object> params) {
        var cypherMapWrapper = CypherMapWrapper.create(params);

        var config = new MLPClassifierTrainConfigImpl(cypherMapWrapper);

        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());

        return config;

    }

    @Override
    @Configuration.Ignore
    default TrainingMethod method() { return TrainingMethod.MLPClassification; }

}
