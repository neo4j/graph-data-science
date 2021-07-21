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
package org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class LinkLogisticRegressionBaseTest {

    @Test
    void shouldComputeCorrectFeatures() {
        var base = new LinkLogisticRegressionBase(LinkLogisticRegressionData.from(1));
        var featureCount = 2;

        var allFeatures = HugeObjectArray.newArray(double[].class, 10, AllocationTracker.empty());


        allFeatures.setAll(idx -> {
            double[] features = new double[featureCount];
            Arrays.fill(features, idx);
            return features;
        });

        Batch batch = new LazyBatch(0, 2, 10);
        Constant<Matrix> batchFeatures = base.features(batch, allFeatures);

        assertThat(batchFeatures.apply(new ComputationContext())).isEqualTo(new Matrix(new double[]{0, 0, 1, 1}, 2, 2));

        Batch otherBatch = new LazyBatch(4, 3, 10);
        Constant<Matrix> otherBatchFeatures = base.features(otherBatch, allFeatures);

        assertThat(otherBatchFeatures.apply(new ComputationContext()))
            .isEqualTo(new Matrix(new double[]{4, 4, 5, 5, 6, 6}, 3, 2));
    }

}