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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LinkLogisticRegressionTrainConfigTest {

    @Test
    void shouldOverrideConcurrency() {
        var foo = LinkLogisticRegressionTrainConfig.of( 4, Map.of("concurrency", 3));
        assertEquals(3, foo.concurrency());
    }

    @Test
    void shouldUseDefaultConcurrency() {
        var foo = LinkLogisticRegressionTrainConfig.of(2, Map.of());
        assertEquals(2, foo.concurrency());
    }

    @Test
    void failOnUnexpectedKeys() {
        assertThatThrownBy(() -> LinkLogisticRegressionTrainConfig.of( 2, Map.of("boogiewoogie", 1))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unexpected configuration key: boogiewoogie");
    }

}
