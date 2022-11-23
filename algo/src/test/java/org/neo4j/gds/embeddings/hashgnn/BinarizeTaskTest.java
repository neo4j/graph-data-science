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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BinarizeTaskTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:N {f1: 1.0, f2: [1.0]})" +
        ", (b:N {f1: -1.0, f2: [1.0]})" +
        ", (c:N {f1: 1.0, f2: [-1.0]})";

    @Inject
    Graph graph;

    @Inject
    IdFunction idFunction;

    @Test
    void shouldPerformHyperplaneRounding() {
        var partition = new Partition(0, graph.nodeCount());
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(4)
            .binarizeFeatures(Map.of("dimension", 4))
            .iterations(100)
            .build();
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, List.of("f1", "f2"));
        var features = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());
        var propertyEmbeddings = new double[][]{{-0.3, 0.1, 0.8, -0.3}, {0.6, 0.2, -0.1, -0.2}};

        new BinarizeTask(
            partition,
            config,
            features,
            featureExtractors,
            propertyEmbeddings,
            ProgressTracker.NULL_TRACKER
        ).run();

        var idA = graph.toMappedNodeId(idFunction.of("a"));
        var idB = graph.toMappedNodeId(idFunction.of("b"));
        var idC = graph.toMappedNodeId(idFunction.of("c"));

        // computed by taking prop matrix * embedding matrix in python
        assertThat(features.get(idA).get(0)).isTrue();
        assertThat(features.get(idA).get(1)).isTrue();
        assertThat(features.get(idA).get(2)).isTrue();
        assertThat(features.get(idA).get(3)).isFalse();

        assertThat(features.get(idB).get(0)).isTrue();
        assertThat(features.get(idB).get(1)).isTrue();
        assertThat(features.get(idB).get(2)).isFalse();
        assertThat(features.get(idB).get(3)).isTrue();

        assertThat(features.get(idC).get(0)).isFalse();
        assertThat(features.get(idC).get(1)).isFalse();
        assertThat(features.get(idC).get(2)).isTrue();
        assertThat(features.get(idC).get(3)).isFalse();

    }


    @Test
    void shouldPerformHyperplaneRoundingWithThreshold() {
        var partition = new Partition(0, graph.nodeCount());
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(4)
            .binarizeFeatures(Map.of("dimension", 4, "threshold", 0.4))
            .iterations(100)
            .build();
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, List.of("f1", "f2"));
        var features = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());
        var propertyEmbeddings = new double[][]{{-0.3, 0.1, 0.8, -0.3}, {0.6, 0.2, -0.1, -0.2}};

        new BinarizeTask(
            partition,
            config,
            features,
            featureExtractors,
            propertyEmbeddings,
            ProgressTracker.NULL_TRACKER
        ).run();

        var idA = graph.toMappedNodeId(idFunction.of("a"));
        var idB = graph.toMappedNodeId(idFunction.of("b"));
        var idC = graph.toMappedNodeId(idFunction.of("c"));

        // computed by taking prop matrix * embedding matrix in python and checking product > threshold
        assertThat(features.get(idA).get(0)).isFalse();
        assertThat(features.get(idA).get(1)).isFalse();
        assertThat(features.get(idA).get(2)).isTrue();
        assertThat(features.get(idA).get(3)).isFalse();

        assertThat(features.get(idB).get(0)).isTrue();
        assertThat(features.get(idB).get(1)).isFalse();
        assertThat(features.get(idB).get(2)).isFalse();
        assertThat(features.get(idB).get(3)).isFalse();

        assertThat(features.get(idC).get(0)).isFalse();
        assertThat(features.get(idC).get(1)).isFalse();
        assertThat(features.get(idC).get(2)).isTrue();
        assertThat(features.get(idC).get(3)).isFalse();

    }

}
