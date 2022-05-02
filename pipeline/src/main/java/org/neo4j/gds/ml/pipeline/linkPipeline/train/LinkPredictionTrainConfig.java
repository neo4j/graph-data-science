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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.metrics.LinkMetric;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.model.ModelConfig;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface LinkPredictionTrainConfig extends AlgoBaseConfig, ModelConfig, RandomSeedConfig {

    String graphName();

    @Value.Default
    @Configuration.DoubleRange(min = 0, minInclusive = false)
    default double negativeClassWeight() {
        return 1.0;
    }

    String pipeline();

    // ConvertWith + default doesnt work (yet)
    default List<String> metrics() {
        return List.of(LinkMetric.AUCPR.name());
    }

    @Configuration.Ignore
    default List<Metric> linkMetrics() {
        return namesToMetrics(metrics());
    }

    static LinkPredictionTrainConfig of(String username, CypherMapWrapper config) {
        return new LinkPredictionTrainConfigImpl(username, config);
    }

    static List<Metric> namesToMetrics(List<String> names) {
        return names.stream().map(name -> LinkMetric.parseLinkMetric((String) name)).collect(Collectors.toList());
    }
}
