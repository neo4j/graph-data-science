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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ComponentSpecTest {

    @Test
    void shouldTurnOffFeature() {
        var falseSpec = ComponentSpec.parse(false);
        assertThat(falseSpec.useComponents()).isFalse();
        assertThat(falseSpec.computeComponents()).isFalse();
    }

    @Test
    void shouldTurnOnFeatureForComputedComponents() {
        var spec = ComponentSpec.parse(true);
        assertThat(spec.useComponents()).isTrue();
        assertThat(spec.computeComponents()).isTrue();
    }

    @Test
    void shouldTurnOnFeatureForPreComputedComponents() {
        var spec = ComponentSpec.parse("prop");
        assertThat(spec.useComponents()).isTrue();
        assertThat(spec.computeComponents()).isFalse();
    }

    @Test
    void shouldThrowOnNullAndEmptyString() {
        assertThatThrownBy(() -> ComponentSpec.parse(""))
            .hasMessageContaining("Invalid component spec: expected a valid node property");
        assertThatThrownBy(() -> ComponentSpec.parse(null))
            .hasMessageContaining("Invalid component spec: cannot parse null as node property");
    }

    @Test
    void shouldThrowOnUnexpectedInputs() {
        assertThatThrownBy(() -> ComponentSpec.parse(42)).hasMessageContaining("Invalid component spec");
    }
}
