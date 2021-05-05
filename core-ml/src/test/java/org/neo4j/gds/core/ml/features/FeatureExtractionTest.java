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
package org.neo4j.gds.core.ml.features;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.functions.MatrixConstant;
import org.neo4j.gds.core.ml.batch.LazyBatch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FeatureExtractionTest extends FeatureExtractionBaseTest {

    @Override
    public void makeExtractions(Graph graph) {
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, List.of("a", "b"));
        graph.forEachNode(nodeId -> {
            FeatureExtraction.extract(nodeId, nodeId, featureExtractors, FeatureConsumer.NOOP);
            return true;
        });
    }

    @Test
    void shouldConcatenateFeatures() {
        var featureExtractors = FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b"));
        var allNodesBatch = new LazyBatch(0, (int) validGraph.nodeCount(), validGraph.nodeCount());
        MatrixConstant featuresMatrix = FeatureExtraction.extract(allNodesBatch, featureExtractors);
        assertThat(featuresMatrix.dimensions()).containsExactly(4, 3);
        assertThat(new ComputationContext().forward(featuresMatrix).data()).contains(
            new double[]{ 2.0, 1.0, 1.2, 1.3, 1.0, 0.5, 0.0, 1.0, 2.8, 1.0, 1.0, 0.9 },
            Offset.offset(1e-7)
        );
    }

    @Test
    void shouldConcatenateFeaturesWithBias() {
        var allExtractors = new ArrayList<>(FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b")));
        allExtractors.add(new BiasFeature());
        var allNodesBatch = new LazyBatch(0, (int) validGraph.nodeCount(), validGraph.nodeCount());
        MatrixConstant featuresMatrix = FeatureExtraction.extract(allNodesBatch, allExtractors);
        assertThat(featuresMatrix.dimensions()).containsExactly(4, 4);
        assertThat(new ComputationContext().forward(featuresMatrix).data()).contains(
            new double[]{ 2.0, 1.0, 1.2, 1.0, 1.3, 1.0, 0.5, 1.0, 0.0, 1.0, 2.8, 1.0, 1.0, 1.0, 0.9, 1.0 },
            Offset.offset(1e-7)
        );
    }

    @Test
    void shouldConcatenateFeaturesHOA() {
        var featureExtractors = FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b"));
        var features = HugeObjectArray.newArray(double[].class, 4, AllocationTracker.empty());
        FeatureExtraction.extract(validGraph, featureExtractors, features);
        assertThat(features.get(0)).contains(
            new double[]{ 2.0, 1.0, 1.2 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(1)).contains(
            new double[]{ 1.3, 1.0, 0.5 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(2)).contains(
            new double[]{ 0.0, 1.0, 2.8 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(3)).contains(
            new double[]{ 1.0, 1.0, 0.9 },
            Offset.offset(1e-7)
        );
    }

    @Test
    void shouldConcatenateFeaturesHOAWithDegreeFeature() {
        var featureExtractors = new ArrayList<>(FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b")));
        featureExtractors.add(new DegreeFeatureExtractor(validGraph));
        var features = HugeObjectArray.newArray(double[].class, 4, AllocationTracker.empty());
        FeatureExtraction.extract(validGraph, featureExtractors, features);
        assertThat(features.get(0)).contains(
            new double[]{ 2.0, 1.0, 1.2, 0.0 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(1)).contains(
            new double[]{ 1.3, 1.0, 0.5, 0.0 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(2)).contains(
            new double[]{ 0.0, 1.0, 2.8, 0.0 },
            Offset.offset(1e-7)
        );
        assertThat(features.get(3)).contains(
            new double[]{ 1.0, 1.0, 0.9, 0.0 },
            Offset.offset(1e-7)
        );
    }

    @Test
    void shouldFailOnMissingArrayPropertyFirstNode() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> FeatureExtraction.propertyExtractors(missingArrayGraph, List.of("a", "b")))
            .withMessageContaining(
                "Missing node property for property key `a` on node with id `0`."
            );
    }

    @Test
    public void shouldCalculateMemoryUsage() throws Exception {
        assertThat(FeatureExtraction.memoryUsageInBytes(1)).isEqualTo(24L);
        assertThat(FeatureExtraction.memoryUsageInBytes(42)).isEqualTo(1008L);
        assertThat(FeatureExtraction.memoryUsageInBytes(43)).isEqualTo(1032L);
    }
}
