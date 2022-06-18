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
package org.neo4j.gds.ml.models.mlp;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.models.FeaturesFactory;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierObjectiveTest {

    @Test
    void shouldCalculateCrossEntropyLoss() {
        var data = MLPClassifierData.create(3,2, false);
        var classifier = new MLPClassifier(data);
        var featuresHOA = HugeObjectArray.of(
            new double[]{0.1, 1.1},
            new double[]{0.2, 1.2},
            new double[]{0.3, 1.3}
        );
        var features = FeaturesFactory.wrap(featuresHOA);
        var labels = HugeIntArray.of(1,2,3);


        var objective = new MLPClassifierObjective(classifier, features, labels);
        var batch = new LazyBatch(0,2,3);
        var loss = objective.loss(batch, 0);
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        assertThat(lossValue).isCloseTo(1.2010265643776676, Offset.offset(1e-08));
    }

}
