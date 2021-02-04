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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LinkLogisticRegressionPredictorTest {

    @Test
    void computesProbability() {
        var predictor = new LinkLogisticRegressionPredictor(null);

        var features = new double[] {.1, .2, .3};
        var weights = new double[] {.5, .6, .7, .8};
        var result = predictor.computeProbability(weights, features);

        var expectedResult = 1 / (1 + Math.pow(Math.E, -1 * (.1 * .5 + .2 * .6 + .3 * .7 + .8)));

        assertThat(result).isCloseTo(expectedResult, Offset.offset(1e-10));
    }
}
