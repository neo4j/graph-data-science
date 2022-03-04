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
package org.neo4j.gds.models.logisticregression;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.gradientdescent.TrainingConfig;

import java.util.Collection;
import java.util.Map;


@Configuration
@SuppressWarnings("immutables:subtype")
public interface LogisticRegressionTrainConfig extends TrainingConfig {

    @Value.Default
    @Configuration.DoubleRange(min = 0.0)
    default double penalty() {
        return 0.0;
    }

    default boolean useBiasFeature() {
        return true;
    }

    @Configuration.ToMap
    Map<String, Object> toMap();

    @Configuration.CollectKeys
    Collection<String> configKeys();

    static LogisticRegressionTrainConfig of(Map<String, Object> params) {
        var cypherMapWrapper = CypherMapWrapper.create(params);

        var config = new LogisticRegressionTrainConfigImpl(cypherMapWrapper);

        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());
        return config;
    }

    static LogisticRegressionTrainConfig defaultConfig() {
        return LogisticRegressionTrainConfig.of(Map.of());
    }
}
