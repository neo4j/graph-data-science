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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScalerFactoryTest {

    @Test
    void parse() {
        assertThat(ScalerFactory.parse("log").type()).isEqualTo(LogScaler.TYPE);
        assertThat(ScalerFactory.parse("minmax").type()).isEqualTo(MinMax.TYPE);
        assertThat(ScalerFactory.parse("max").type()).isEqualTo(Max.TYPE);
        assertThat(ScalerFactory.parse("center").type()).isEqualTo(Center.TYPE);
        assertThat(ScalerFactory.parse("l1norm").type()).isEqualTo(L1Norm.TYPE);
        assertThat(ScalerFactory.parse("l2norm").type()).isEqualTo(L2Norm.TYPE);
        assertThat(ScalerFactory.parse("mean").type()).isEqualTo(Mean.TYPE);
        assertThat(ScalerFactory.parse("stdscore").type()).isEqualTo(StdScore.TYPE);
        assertThat(ScalerFactory.parse("none").type()).isEqualTo(NoneScaler.TYPE);

        // case insensitive
        assertThat(ScalerFactory.parse("L1NORM").type()).isEqualTo(L1Norm.TYPE);
        assertThat(ScalerFactory.parse("StdScore").type()).isEqualTo(StdScore.TYPE);

        // nested syntax
        assertThat(ScalerFactory.parse(Map.of("type", "log")).type()).isEqualTo(LogScaler.TYPE);
        assertThat(ScalerFactory.parse(Map.of("type", "log", "offset", 10)).type()).isEqualTo(LogScaler.TYPE);
        assertThat(ScalerFactory.parse(Map.of("type", "minmax")).type()).isEqualTo(MinMax.TYPE);
        assertThat(ScalerFactory.parse(Map.of("type", "STDSCORE")).type()).isEqualTo(StdScore.TYPE);
        assertThat(ScalerFactory.parse(Map.of("type", "CEntEr")).type()).isEqualTo(Center.TYPE);
    }

    @Test
    void badInput() {
        // bad strings
        assertThatThrownBy(() -> ScalerFactory.parse("mean  ")).hasMessageContaining("Unrecognised scaler type specified: `mean  `.");
        assertThatThrownBy(() -> ScalerFactory.parse("yo")).hasMessageContaining("Unrecognised scaler type specified: `yo`.");

        // bad types
        assertThatThrownBy(() -> ScalerFactory.parse(1L)).hasMessageContaining("Unrecognised scaler type specified: `1`.");
        assertThatThrownBy(() -> ScalerFactory.parse(42D)).hasMessageContaining("Unrecognised scaler type specified: `42.0`.");
        assertThatThrownBy(() -> ScalerFactory.parse(List.of("mean"))).hasMessageContaining("Unrecognised scaler type specified: `[mean]`.");
        assertThatThrownBy(() -> ScalerFactory.parse(Map.of("mean", "scaler"))).hasMessageContaining("Unrecognised scaler type specified: `{mean=scaler}`.");
        assertThatThrownBy(() -> ScalerFactory.parse(false)).hasMessageContaining("Unrecognised scaler type specified: `false`.");

        // bad nested syntax
        assertThatThrownBy(() -> ScalerFactory.parse(Map.of("type", "lag"))).hasMessageContaining("Unrecognised scaler type specified: `lag`.");
        assertThatThrownBy(() -> ScalerFactory.parse(Map.of("type", "log", "offset", false))).hasMessageContaining("The value of `offset` must be of type `Number` but was `Boolean`.");
        assertThatThrownBy(() -> ScalerFactory.parse(Map.of("type", "log", "offsat", 0))).hasMessageContaining("Unexpected configuration key: offsat");
    }
}
