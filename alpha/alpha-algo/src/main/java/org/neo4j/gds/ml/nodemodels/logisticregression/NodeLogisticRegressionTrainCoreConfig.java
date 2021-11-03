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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.TrainingConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Configuration
// This class is currently used internally in NodeClassification and is not
// a procedure-level configuration. it is derived from a NodeClassificationTrainConfig
public interface NodeLogisticRegressionTrainCoreConfig extends TrainingConfig {

    @Configuration.DoubleRange(min = 0.0)
    double penalty();

    @Configuration.CollectKeys
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    @Configuration.ToMap
    Map<String, Object> toMap();

    static NodeLogisticRegressionTrainCoreConfig of(
        Map<String, Object> params
    ) {
        var cypherMapWrapper = CypherMapWrapper.create(params);

        var config = new NodeLogisticRegressionTrainCoreConfigImpl(cypherMapWrapper);

        cypherMapWrapper.requireOnlyKeysFrom(config.configKeys());
        return config;
    }

    static NodeLogisticRegressionTrainCoreConfig defaultConfig() {
        return NodeLogisticRegressionTrainCoreConfig
            .of(Map.of("penalty", 0.0));
    }
}
