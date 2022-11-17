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
package org.neo4j.gds.ml.models.logisticregression;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogisticRegressionTrainConfigTest {

    @Test
    void failOnUnexpectedKeys() {
        assertThatThrownBy(() -> LogisticRegressionTrainConfig.of(Map.of("boogiewoogie", 1, "methodName", "myMethod")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unexpected configuration keys: boogiewoogie, methodName");
    }

    @Test
    void shouldInitializeCorrectClassWeights() {
        assertThat(LogisticRegressionTrainConfigImpl.builder().build().initializeClassWeights(5))
            .isEqualTo(new double[]{1,1,1,1,1});

        assertThat(LogisticRegressionTrainConfigImpl.builder().classWeights(List.of(1.5, 0.5)).build().initializeClassWeights(2))
            .isEqualTo(new double[]{1.5, 0.5});

        assertThatThrownBy(() -> LogisticRegressionTrainConfigImpl.builder().classWeights(List.of(1.5, 0.5)).build().initializeClassWeights(5))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The classWeights list [1.5, 0.5] has 2 entries, but it should have 5 entries instead, which is the number of classes.");
    }
}
