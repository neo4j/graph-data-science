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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.features.FeatureExtraction;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.fastrp.ImmutableFastRPExtendedStreamConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
public class FastRPGdlTest {


    @GdlGraph(graphNamePrefix = "array")
    private static final String X =
        "CREATE" +
        "  (a:N {f: [0.4, 1.3, 1.4]})" +
        ", (b:N {f: [2.1, 0.5, 1.8]})" +
        ", (c:N {f: [-0.3, 0.8, 2.8]})" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @GdlGraph(graphNamePrefix = "scalar")
    private static final String Y =
        "CREATE" +
        "  (a:N {f1: 0.4, f2: 1.3, f3: 1.4})" +
        ", (b:N {f1: 2.1, f2: 0.5, f3: 1.8})" +
        ", (c:N {f1: -0.3, f2: 0.8, f3: 2.8})" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @Inject
    Graph scalarGraph;

    @Inject
    Graph arrayGraph;

    @Test
    void shouldYieldSameResultsForScalarAndArrayProperties() {
        assert arrayGraph.nodeCount() == scalarGraph.nodeCount();
        var arrayProperties = List.of("f");
        var arrayEmbeddings = embeddings(arrayGraph, arrayProperties);
        var scalarProperties = List.of("f1", "f2", "f3");
        var scalarEmbeddings = embeddings(scalarGraph, scalarProperties);
        for (int i = 0; i < arrayGraph.nodeCount(); i++) {
            assertThat(arrayEmbeddings.get(i)).contains(scalarEmbeddings.get(i));
        }
    }

    private HugeObjectArray<float[]> embeddings(Graph graph, List<String> properties) {
        var arrayConfig = ImmutableFastRPExtendedStreamConfig.builder()
            .addAllFeatureProperties(properties)
            .embeddingDimension(64)
            .propertyDimension(32)
            .build();

        var fastRPArray = new FastRP(
            graph,
            arrayConfig,
            FeatureExtraction.propertyExtractors(graph, properties),
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty(),
            Optional.of(123L)
        );
        return fastRPArray.compute().embeddings();
    }
}
