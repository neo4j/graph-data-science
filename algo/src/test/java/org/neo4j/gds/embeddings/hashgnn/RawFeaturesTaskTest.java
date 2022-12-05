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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class RawFeaturesTaskTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:N {f1: 1, f2: [1.0, 1.0]})" +
        ", (b:N {f1: 1, f2: [1.0, 0.0]})" +
        ", (c:N {f1: 1, f2: [0.0, 1.0]})";

    @GdlGraph(graphNamePrefix = "nonBinary")
    private static final String NON_BINARY =
        "CREATE" +
        "  (a:N {f1: 1, f2: [1.0, 1.0]})" +
        ", (b:N {f1: 1, f2: [1.0, 0.0000000001]})" +
        ", (c:N {f1: 1, f2: [0.0, 1.0]})";

    @Inject
    private Graph graph;

    @Inject
    private Graph nonBinaryGraph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldFailOnNonBinaryFeatures() {
        var partition = new Partition(0, nonBinaryGraph.nodeCount());
        var featureExtractors = FeatureExtraction.propertyExtractors(nonBinaryGraph, List.of("f1", "f2"));
        var features = HugeObjectArray.newArray(HugeAtomicBitSet.class, nonBinaryGraph.nodeCount());
        var inputDimension = FeatureExtraction.featureCount(featureExtractors);

        assertThatThrownBy(() -> {
            new RawFeaturesTask(
                partition,
                nonBinaryGraph,
                featureExtractors,
                inputDimension,
                features,
                ProgressTracker.NULL_TRACKER
            ).run();
        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Feature properties may only contain values 0 and 1 unless `binarizeFeatures` is used. Node 1 and possibly other nodes have a feature property containing value 0.00000000010000000");
    }
    @Test
    void shouldPickCorrectFeatures() {
        var partition = new Partition(0, graph.nodeCount());
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, List.of("f1", "f2"));
        var features = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());
        var inputDimension = FeatureExtraction.featureCount(featureExtractors);

        new RawFeaturesTask(
            partition,
            graph,
            featureExtractors,
            inputDimension,
            features,
            ProgressTracker.NULL_TRACKER
        ).run();

        var idA = graph.toMappedNodeId(idFunction.of("a"));
        var idB = graph.toMappedNodeId(idFunction.of("b"));
        var idC = graph.toMappedNodeId(idFunction.of("c"));

        assertThat(features.get(idA).get(0)).isTrue();
        assertThat(features.get(idA).get(1)).isTrue();
        assertThat(features.get(idA).get(2)).isTrue();

        assertThat(features.get(idB).get(0)).isTrue();
        assertThat(features.get(idB).get(1)).isTrue();
        assertThat(features.get(idB).get(2)).isFalse();

        assertThat(features.get(idC).get(0)).isTrue();
        assertThat(features.get(idC).get(1)).isFalse();
        assertThat(features.get(idC).get(2)).isTrue();
    }

}
