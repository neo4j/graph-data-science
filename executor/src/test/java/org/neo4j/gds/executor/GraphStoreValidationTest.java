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
package org.neo4j.gds.executor;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.gdl.GdlFactory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphStoreValidationTest {

    @SuppressWarnings("JUnit5MalformedNestedClass")
    @Nested
    static class MutatePropertyConfigTests {
        @Test
        void testMutateFailsOnExistingToken() {
            var graphStore = GdlFactory.of("(a {foo: 42})").build();
            var config = ImmutableTestMutatePropertyConfig.builder().mutateProperty("foo").build();

            assertThatThrownBy(() -> GraphStoreValidation.validate(graphStore, config))
                .hasMessageContaining("Node property `foo` already exists in the in-memory graph.");
        }

        @ValueClass
        interface TestMutatePropertyConfig extends AlgoBaseConfig, MutatePropertyConfig {}
    }
}
