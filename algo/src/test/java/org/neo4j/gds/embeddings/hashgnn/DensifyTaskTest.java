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
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DensifyTaskTest {
    @Test
    void shouldDensify() {
        var nodeCount = 3;

        var partition = new Partition(0, nodeCount);
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(4)
            .binarizeFeatures(Map.of("dimension", 4, "densityLevel", 1))
            .outputDimension(5)
            .iterations(100)
            .build();
        var denseFeatures = HugeObjectArray.newArray(double[].class, nodeCount);
        var binaryFeatures = HugeObjectArray.newArray(HugeAtomicBitSet.class, nodeCount);
        binaryFeatures.set(0, HugeAtomicBitSet.create(3));
        binaryFeatures.get(0).set(0);
        binaryFeatures.get(0).set(1);
        binaryFeatures.get(0).set(2);

        binaryFeatures.set(1, HugeAtomicBitSet.create(3));
        binaryFeatures.get(1).set(0);
        binaryFeatures.get(1).set(1);

        binaryFeatures.set(2, HugeAtomicBitSet.create(3));
        binaryFeatures.get(2).set(0);
        binaryFeatures.get(2).set(2);
        var projectionMatrix = new float[][]{
            {1.1f, 1.0f, -1.0f, 0.0f, 0.0f},
            {0.0f, -0.9f, 1.0f, 0.0f, 1.0f},
            {0.0f, 0.0f, 1.0f, -1.0f, -1.0f}
        };

        new DensifyTask(
            partition,
            config,
            denseFeatures,
            binaryFeatures,
            projectionMatrix,
            ProgressTracker.NULL_TRACKER
        ).run();

        assertThat(denseFeatures.get(0)).usingComparatorWithPrecision(1e-7).containsExactly(1.1, 0.1, 1.0, -1.0, 0.0);
        assertThat(denseFeatures.get(1)).usingComparatorWithPrecision(1e-7).containsExactly(1.1, 0.1, 0.0, 0.0, 1.0);
        assertThat(denseFeatures.get(2)).usingComparatorWithPrecision(1e-7).containsExactly(1.1, 1.0, 0.0, -1.0, -1.0);
    }
}
