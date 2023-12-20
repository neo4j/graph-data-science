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
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@GdlExtension
class KnnConfigTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a { knn: 1.2, prop: 1.0 } )" +
            ", (b { knn: 1.1, prop: 5.0 } )" +
            ", (c { knn: 42.0, prop: 10.0 } )";
    @Inject
    private TestGraph graph;

    @Test
    void shouldRenderNodePropertiesWithResolvedDefaultMetrics() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("knn")
            )
        );
        var knnConfig = new KnnBaseConfigImpl(userInput);

        // Initializing the similarity computer causes the default metric to be resolved
        SimilarityComputer.ofProperties(graph, knnConfig.nodeProperties());

        assertThat(knnConfig.toMap().get("nodeProperties")).isEqualTo(
            Map.of(
                "knn", SimilarityMetric.DOUBLE_PROPERTY_METRIC.name()
            )
        );
    }

    @Test
    void invalidRandomParameters() {
        var configBuilder = ImmutableKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("dummy")))
            .concurrency(4)
            .randomSeed(1337L);
        assertThrows(IllegalArgumentException.class, configBuilder::build);
    }
}
