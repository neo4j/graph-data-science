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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.mem.MemoryTree;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MemoryUsageValidatorTest extends BaseTest {

    @Test
    void shouldPassOnSufficientMemory() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = MemoryTree.empty();

        assertThatNoException().isThrownBy(() -> new MemoryUsageValidator(new MemoryTracker(10000000, Log.noOpLog()),
            false,
            Log.noOpLog()
        )
            .tryValidateMemoryUsage(
                TestConfig.empty(),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions)
            ));
    }

    @Test
    void shouldFailOnInsufficientMemory() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));

        assertThatThrownBy(() -> new MemoryUsageValidator(new MemoryTracker(21, Log.noOpLog()),
            false,
            Log.noOpLog()
        )
            .tryValidateMemoryUsage(
                TestConfig.empty(),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions)
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since minimum estimated memory (42 Bytes) exceeds current free memory (21 Bytes).");
    }

    @Test
    void shouldNotFailOnInsufficientMemoryIfInSudoMode() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));

        assertThatNoException().isThrownBy(() -> new MemoryUsageValidator(new MemoryTracker(21, Log.noOpLog()),
            false,
            Log.noOpLog()
        )
            .tryValidateMemoryUsage(
                TestConfig.of(CypherMapWrapper.empty().withBoolean("sudo", true)),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions)
            ));
    }

    @Test
    void shouldLogWhenFailing() {
        var log = mock(Log.class);
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));
        var memoryUsageValidator = new MemoryUsageValidator(
            new MemoryTracker(21, log), false, log
        );

        assertThatIllegalStateException().isThrownBy(
            () -> memoryUsageValidator.tryValidateMemoryUsage(
                TestConfig.of(CypherMapWrapper.empty()),
                (config -> new MemoryTreeWithDimensions(memoryTree, dimensions))
            )
        );

        verify(log).info("Procedure was blocked since minimum estimated memory (42 Bytes) exceeds current free memory (21 Bytes).");
    }

    private static final GraphDimensions TEST_DIMENSIONS = ImmutableGraphDimensions
        .builder()
        .nodeCount(100)
        .relCountUpperBound(1000)
        .build();

    static Stream<Arguments> input() {
        var fixedMemory = MemoryEstimations.builder().fixed("foobar", 1337);
        var memoryRange = MemoryEstimations
            .builder()
            .rangePerGraphDimension("foobar", (dimensions, concurrency) -> MemoryRange.of(42, 1337));
        return Stream.of(
            Arguments.of(fixedMemory.build(), false),
            Arguments.of(fixedMemory.build(), true),
            Arguments.of(memoryRange.build(), false),
            Arguments.of(memoryRange.build(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("input")
    void doesNotThrow(MemoryEstimation estimation, boolean useMaxMemoryUsage) {
        var memoryTrackerMock = mock(MemoryTracker.class);
        var memoryUsageValidator = new MemoryUsageValidator(
            memoryTrackerMock,
            false,
            Log.noOpLog()
        );
            var memoryTree = estimation.estimate(TEST_DIMENSIONS, new Concurrency(1));
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertDoesNotThrow(() -> memoryUsageValidator.validateMemoryUsage(
            memoryTreeWithDimensions.memoryTree.memoryUsage(), 10_000,
            useMaxMemoryUsage,
            new JobId("foo"), Log.noOpLog()
        ));
    }

    @ParameterizedTest
    @MethodSource("input")
    void throwsOnMinUsageExceeded(MemoryEstimation estimation, boolean ignored) {
        var memoryUsageValidator = new MemoryUsageValidator(
            null,
            false,
            Log.noOpLog()
        );

        var memoryTree = estimation.estimate(TEST_DIMENSIONS, new Concurrency(1));
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertThatThrownBy(() -> memoryUsageValidator.validateMemoryUsage(
            memoryTreeWithDimensions.memoryTree.memoryUsage(), 1,
            false,
            new JobId("foo"), Log.noOpLog()
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since minimum estimated memory");
    }

    @ParameterizedTest
    @MethodSource("input")
    void throwsOnMaxUsageExceeded(MemoryEstimation estimation, boolean ignored) {
        var memoryUsageValidator = new MemoryUsageValidator(
            null,
            false,
            Log.noOpLog()
        );

        var memoryTree = estimation.estimate(TEST_DIMENSIONS, new Concurrency(1));
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertThatThrownBy(() -> memoryUsageValidator.validateMemoryUsage(
            memoryTreeWithDimensions.memoryTree.memoryUsage(), 1,
            true,
            new JobId("foo"), Log.noOpLog()
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since maximum estimated memory")
            .hasMessageContaining(
                "Consider resizing your Aura instance via console.neo4j.io. " +
                    "Alternatively, use 'sudo: true' to override the memory validation. " +
                    "Overriding the validation is at your own risk. " +
                    "The database can run out of memory and data can be lost."
            );
    }

    @Configuration
    interface TestConfig extends AlgoBaseConfig {
        static TestConfig empty() {
            return new TestConfigImpl(CypherMapWrapper.empty());
        }

        static TestConfig of(CypherMapWrapper map) {
            return new TestConfigImpl(map);
        }
    }

    public static class TestTree implements MemoryTree {
        private final String description;
        private final MemoryRange range;

        TestTree(final String description, final MemoryRange range) {
            this.description = description;
            this.range = range;
        }

        @Override
        public MemoryRange memoryUsage() {
            return range;
        }

        @Override
        public String description() {
            return description;
        }
    }
}
