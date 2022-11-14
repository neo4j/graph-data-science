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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class MLPClassifierTrainConfigTest {

    @Test
    void failOnUnexpectedKeys() {
        assertThatThrownBy(() -> MLPClassifierTrainConfig.of(Map.of("wrongkey1", 615, "wrongkey2", "value2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unexpected configuration keys: wrongkey1, wrongkey2");
    }

    @Test
    void checkMLPSpecificConfigIsSet() {
        var MLPConfig = MLPClassifierTrainConfig.of(Map.of("hiddenLayerSizes", List.of(42,10)));
        assertThat(MLPConfig.hiddenLayerSizes()).isEqualTo(List.of(42,10));
    }

    @Test
    void checkHiddenLayerSizesTyoe() {
        assertThatThrownBy(() -> MLPClassifierTrainConfig.of(Map.of("hiddenLayerSizes", 42)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldInitializeClassWeights() {
        Assertions.assertThat(MLPClassifierTrainConfigImpl.builder().build().initializeClassWeights(5))
            .isEqualTo(new double[]{1,1,1,1,1});

        Assertions.assertThat(MLPClassifierTrainConfigImpl.builder().classWeights(List.of(0.8, 0.2)).build().initializeClassWeights(5))
            .isEqualTo(new double[]{0.8, 0.2});

        assertThatThrownBy(() -> MLPClassifierTrainConfigImpl.builder().classWeights(List.of(0.5, 0.2)).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The class weights [0.5, 0.2] sum up to 0.7, they should sum up to 1 instead.");
    }

}
