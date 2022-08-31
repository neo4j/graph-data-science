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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPipelineBaseTrainConfig;

import java.util.List;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface NodeRegressionPipelineTrainConfig extends NodePropertyPipelineBaseTrainConfig {

    @Configuration.ConvertWith("org.neo4j.gds.ml.metrics.regression.RegressionMetrics#parseList")
    @Configuration.ToMapValue("org.neo4j.gds.ml.metrics.regression.RegressionMetrics#toString")
    List<RegressionMetrics> metrics();

    static NodeRegressionPipelineTrainConfig of(String username, CypherMapWrapper config) {
        return new NodeRegressionPipelineTrainConfigImpl(username, config);
    }

    @Value.Check
    default void validateMetrics() {
        if (metrics().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one evaluation metric via the `metrics` parameter.");
        }
    }
}
