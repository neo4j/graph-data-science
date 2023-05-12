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
package org.neo4j.gds.ml.core.features;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.batch.RangeBatch;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        var allNodesBatch = new RangeBatch(0, (int) validGraph.nodeCount(), validGraph.nodeCount());
        var featuresMatrix = FeatureExtraction.extract(allNodesBatch, featureExtractors);

        var expected = new Matrix(new double[]{ 2.0, 1.0, 1.2, 1.3, 1.0, 0.5, 0.0, 1.0, 2.8, 1.0, 1.0, 0.9 }, 4, 3);
        assertThat(featuresMatrix.data()).matches(matrix -> matrix.equals(expected, 1e-7));
    }

    @Test
    void shouldConcatenateFeaturesWithBias() {
        var allExtractors = new ArrayList<>(FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b")));
        allExtractors.add(new BiasFeature());
        var allNodesBatch = new RangeBatch(0, (int) validGraph.nodeCount(), validGraph.nodeCount());
        var featuresMatrix = FeatureExtraction.extract(allNodesBatch, allExtractors);

        var expected = new Matrix(new double[]{2.0, 1.0, 1.2, 1.0, 1.3, 1.0, 0.5, 1.0, 0.0, 1.0, 2.8, 1.0, 1.0, 1.0, 0.9, 1.0}, 4, 4);
        assertThat(featuresMatrix.data()).matches(matrix -> matrix.equals(expected, 1e-7));
    }

    @Test
    void shouldConcatenateFeaturesHOA() {
        var featureExtractors = FeatureExtraction.propertyExtractors(validGraph, List.of("a", "b"));
        var features = HugeObjectArray.newArray(double[].class, 4);
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
        var features = HugeObjectArray.newArray(double[].class, 4);
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
    void shouldFailOnMissingArrayPropertyFirstNode() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> FeatureExtraction.propertyExtractors(missingArrayGraph, List.of("a", "b")))
            .withMessageContaining(
                String.format(
                    Locale.US,
                "Missing node property for property key `a` on node with id `%s`.",
                    missingArrayGraph.toOriginalNodeId("n1")
            ));
    }

    @Test
    public void shouldCalculateMemoryUsage() {
        assertThat(FeatureExtraction.memoryUsageInBytes(1)).isEqualTo(32L);
        assertThat(FeatureExtraction.memoryUsageInBytes(42)).isEqualTo(32L * 42L);
        assertThat(FeatureExtraction.memoryUsageInBytes(43)).isEqualTo(32L * 43L);
    }
}
