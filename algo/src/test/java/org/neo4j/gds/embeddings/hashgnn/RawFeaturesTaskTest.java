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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RawFeaturesTaskTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:N {f1: 1, f2: [1.0, 1.0]})" +
        ", (b:N {f1: 1, f2: [1.0, 0.0]})" +
        ", (c:N {f1: 1, f2: [0.0, 1.0]})";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldPickCorrectFeatures() {
        var partition = new Partition(0, graph.nodeCount());
        var config = HashGNNConfigImpl.builder().featureProperties(List.of("f1", "f2")).embeddingDensity(2).iterations(100).build();
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, List.of("f1", "f2"));
        var features = HugeObjectArray.newArray(BitSet.class, graph.nodeCount());
        var inputDimension = FeatureExtraction.featureCount(featureExtractors);
        var hashes = List.of(new int[] {4, 2, 9}, new int[] {6, 2, 1});

        new RawFeaturesTask(partition, config, featureExtractors, inputDimension, features, hashes).run();

        var idA = graph.toMappedNodeId(idFunction.of("a"));
        var idB = graph.toMappedNodeId(idFunction.of("b"));
        var idC = graph.toMappedNodeId(idFunction.of("c"));

        assertThat(features.get(idA).get(0)).isFalse();
        assertThat(features.get(idA).get(1)).isTrue();
        assertThat(features.get(idA).get(2)).isTrue();

        assertThat(features.get(idB).get(0)).isFalse();
        assertThat(features.get(idB).get(1)).isTrue();
        assertThat(features.get(idB).get(2)).isFalse();

        assertThat(features.get(idC).get(0)).isTrue();
        assertThat(features.get(idC).get(1)).isFalse();
        assertThat(features.get(idC).get(2)).isTrue();
    }

}
