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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierDataTest {

    @Test
    void shouldCreateData() {
        var hiddenLayerSizes = List.of(24,18,12);
        var data = MLPClassifierData.create(3, 4, hiddenLayerSizes, new SplittableRandom(0l));

        var weights = data.weights();
        var biases = data.biases();

        assertThat(weights.size()).isEqualTo(4);
        assertThat(biases.size()).isEqualTo(4);

        assertThat(weights.get(0).data().dimensions()).containsExactly(24,4);
        assertThat(weights.get(1).data().dimensions()).containsExactly(18, 24);
        assertThat(weights.get(2).data().dimensions()).containsExactly(12,18);
        assertThat(weights.get(3).data().dimensions()).containsExactly(3,12);

        assertThat(biases.get(0).data().length()).isEqualTo(24);
        assertThat(biases.get(1).data().length()).isEqualTo(18);
        assertThat(biases.get(2).data().length()).isEqualTo(12);
        assertThat(biases.get(3).data().length()).isEqualTo(3);
    }

}
