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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.l2Normalize;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;

@GdlExtension
public class FastRPGdlTest {

    private static final int DEFAULT_EMBEDDING_DIMENSION = 128;
    private static final FastRPBaseConfig DEFAULT_CONFIG = FastRPBaseConfig.builder()
            .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
            .propertyRatio(0.5)
            .featureProperties(List.of("f1", "f2", "f3"))
            .addIterationWeight(1.0D)
            .randomSeed(42L)
            .build();

    @GdlGraph(graphNamePrefix = "array")
    private static final String X =
            "CREATE" +
                    "  (a:Node1 {f: [0.4, 1.3, 1.4]})" +
                    ", (b:Node1 {f: [2.1, 0.5, 1.8]})" +
                    ", (c:Node2 {f: [-0.3, 0.8, 2.8]})" +
                    ", (a)-[:REL {weight: 2.0}]->(b)" +
                    ", (b)-[:REL {weight: 1.0}]->(a)" +
                    ", (a)-[:REL {weight: 1.0}]->(c)" +
                    ", (c)-[:REL {weight: 1.0}]->(a)" +
                    ", (b)-[:REL {weight: 1.0}]->(c)" +
                    ", (c)-[:REL {weight: 1.0}]->(b)";

    @GdlGraph(graphNamePrefix = "scalar")
    private static final String Y =
            "CREATE" +
                    "  (a:Node1 {f1: 0.4, f2: 1.3, f3: 1.4})" +
                    ", (b:Node1 {f1: 2.1, f2: 0.5, f3: 1.8})" +
                    ", (c:Node2 {f1: -0.3, f2: 0.8, f3: 2.8})" +
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

    @Inject
    GraphStore scalarGraphStore;

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

    @Test
    void shouldSwapInitialRandomVectors() {
        final var graph = scalarGraphStore.getGraph(
                NodeLabel.of("Node1"),
                RelationshipType.of("REL"),
                Optional.empty()
        );

        FastRP fastRP = new FastRP(
                graph,
                DEFAULT_CONFIG,
                defaultFeatureExtractors(graph),
                ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 2);
        fastRP.currentEmbedding(-1).copyTo(randomVectors, 2);
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();

        float[] expected = randomVectors.get(1);
        l2Normalize(expected);

        assertThat(embeddings.get(0)).isEqualTo(expected);
    }

    @Test
    void shouldAverageNeighbors() {
        final var graph = scalarGraphStore.getGraph(
                List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
                List.of(RelationshipType.of("REL")),
                Optional.empty()
        );

        FastRP fastRP = new FastRP(
                graph,
                DEFAULT_CONFIG,
                defaultFeatureExtractors(graph),
                ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 3);
        fastRP.currentEmbedding(-1).copyTo(randomVectors, 3);
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();

        float[] expected = new float[DEFAULT_EMBEDDING_DIMENSION];
        for (int i = 0; i < DEFAULT_EMBEDDING_DIMENSION; i++) {
            expected[i] = (randomVectors.get(1)[i] + randomVectors.get(2)[i]) / 2.0f;
        }
        l2Normalize(expected);

        assertThat(embeddings.get(0)).containsExactly(expected);
    }

    private HugeObjectArray<float[]> embeddings(Graph graph, List<String> properties) {
        var fastRPArray = new FastRP(
                graph,
                DEFAULT_CONFIG,
                FeatureExtraction.propertyExtractors(graph, properties),
                ProgressTracker.NULL_TRACKER
        );
        return fastRPArray.compute().embeddings();
    }

    private List<FeatureExtractor> defaultFeatureExtractors(Graph graph) {
        return FeatureExtraction.propertyExtractors(graph, DEFAULT_CONFIG.featureProperties());
    }
}
