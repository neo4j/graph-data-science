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
package org.neo4j.gds.configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class LimitFactoryTest {
    @Test
    void shouldCreateLongLimit() {
        Limit limit = LimitFactory.create(42L);

        assertThat(limit.isViolated(23L)).isFalse();
        assertThat(limit.isViolated(42L)).isFalse();
        assertThat(limit.isViolated(43L)).isTrue();
        assertThat(limit.isViolated(87L)).isTrue();
    }

    @Test
    void shouldCreateDoubleLimit() {
        Limit limit = LimitFactory.create(2.71828182846);

        assertThat(limit.isViolated(1.61803398875)).isFalse();
        assertThat(limit.isViolated(2.71828182846)).isFalse();
        assertThat(limit.isViolated(2.8)).isTrue();
        assertThat(limit.isViolated(3.14159265359)).isTrue();
    }

    @Test
    void shouldCreateBooleanLimit() {
        Limit limit = LimitFactory.create(false);

        assertThat(limit.isViolated(false)).isFalse();
        assertThat(limit.isViolated(true)).isTrue();

        limit = LimitFactory.create(true);

        assertThat(limit.isViolated(false)).isTrue();
        assertThat(limit.isViolated(true)).isFalse();
    }

    @Test
    void shouldFailMeaningfully() {
        try {
            LimitFactory.create("no good");

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Unable to create limit for input value 'no good' (string)");
        }
    }
}
