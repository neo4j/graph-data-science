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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MutateStubConfigurationValidationDecoratorTest {
    @Test
    void shouldFailToParseConfigurationWhenKeyIsPresent() {
        var decorator = new MutateStubConfigurationValidationDecorator<>(null, "some key");

        try {
            decorator.parseConfiguration(Map.of("some key", "some value"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Unexpected configuration key: some key");
        }
    }

    @Test
    void shouldAllowParsingConfigurationWhenKeyNotPresent() {
        var stub = mock(ExampleMutateStub.class);
        var decorator = new MutateStubConfigurationValidationDecorator<>(stub, "some key");

        when(stub.parseConfiguration(emptyMap())).thenReturn("some configuration");
        var configuration = decorator.parseConfiguration(emptyMap());

        assertThat(configuration).isEqualTo("some configuration");
    }

    @Test
    void shouldFailToGetMemoryEstimationWhenKeyIsPresent() {
        var decorator = new MutateStubConfigurationValidationDecorator<>(null, "some key");

        try {
            decorator.getMemoryEstimation("some user", Map.of("some key", "some value"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Unexpected configuration key: some key");
        }
    }

    @Test
    void shouldAllowGettingMemoryEstimationWhenKeyNotPresent() {
        var stub = mock(ExampleMutateStub.class);
        var decorator = new MutateStubConfigurationValidationDecorator<>(stub, "some key");

        var memoryEstimation = mock(MemoryEstimation.class);
        when(stub.getMemoryEstimation("some user", emptyMap())).thenReturn(memoryEstimation);
        var actualMemoryEstimation = decorator.getMemoryEstimation("some user", emptyMap());

        assertThat(actualMemoryEstimation).isSameAs(memoryEstimation);
    }

    @Test
    void shouldFailToEstimateWhenKeyIsPresent() {
        var decorator = new MutateStubConfigurationValidationDecorator<>(null, "some key");

        try {
            decorator.estimate("some graph", Map.of("some key", "some value"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Unexpected configuration key: some key");
        }
    }

    @Test
    void shouldAllowEstimatingWhenKeyNotPresent() {
        var stub = mock(ExampleMutateStub.class);
        var decorator = new MutateStubConfigurationValidationDecorator<>(stub, "some key");

        var result = Stream.of(mock(MemoryEstimateResult.class));
        when(stub.estimate("some graph", emptyMap())).thenReturn(result);
        var actualResult = decorator.estimate("some graph", emptyMap());

        assertThat(actualResult).isSameAs(result);
    }

    @Test
    void shouldFailToExecuteWhenKeyIsPresent() {
        var decorator = new MutateStubConfigurationValidationDecorator<>(null, "some key");

        try {
            decorator.execute("some graph", Map.of("some key", "some value"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Unexpected configuration key: some key");
        }
    }

    @Test
    void shouldAllowExecutingWhenKeyNotPresent() {
        var stub = mock(ExampleMutateStub.class);
        var decorator = new MutateStubConfigurationValidationDecorator<>(stub, "some key");

        var result = Stream.of(mock(Void.class));
        when(stub.execute("some graph", emptyMap())).thenReturn(result);
        var actualResult = decorator.execute("some graph", emptyMap());

        assertThat(actualResult).isSameAs(result);
    }
}
