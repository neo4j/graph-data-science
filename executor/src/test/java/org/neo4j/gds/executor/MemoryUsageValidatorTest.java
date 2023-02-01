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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemoryUsageValidatorTest extends BaseTest {

    @Test
    void shouldPassOnSufficientMemory() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = MemoryTree.empty();

        assertThatNoException().isThrownBy(() -> new MemoryUsageValidator(Neo4jProxy.testLog(), GraphDatabaseApiProxy.dependencyResolver(db))
            .tryValidateMemoryUsage(
                TestConfig.empty(),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions),
                () -> 10000000
            ));
    }

    @Test
    void shouldFailOnInsufficientMemory() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));

        assertThatThrownBy(() -> new MemoryUsageValidator(Neo4jProxy.testLog(), GraphDatabaseApiProxy.dependencyResolver(db))
            .tryValidateMemoryUsage(
                TestConfig.empty(),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions),
                () -> 21
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since minimum estimated memory (42 Bytes) exceeds current free memory (21 Bytes).");
    }

    @Test
    void shouldNotFailOnInsufficientMemoryIfInSudoMode() {
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));

        assertThatNoException().isThrownBy(() -> new MemoryUsageValidator(Neo4jProxy.testLog(), GraphDatabaseApiProxy.dependencyResolver(db))
            .tryValidateMemoryUsage(
                TestConfig.of(CypherMapWrapper.empty().withBoolean("sudo", true)),
                (config) -> new MemoryTreeWithDimensions(memoryTree, dimensions),
                () -> 21
            ));
    }

    @Test
    void shouldLogWhenFailing() {
        var log = Neo4jProxy.testLog();
        var dimensions = GraphDimensions.builder().nodeCount(1000).build();
        var memoryTree = new TestTree("test", MemoryRange.of(42));
        var memoryUsageValidator = new MemoryUsageValidator(log, GraphDatabaseApiProxy.dependencyResolver(db));
        try {
            memoryUsageValidator.tryValidateMemoryUsage(
                TestConfig.of(CypherMapWrapper.empty()),
                (config -> new MemoryTreeWithDimensions(memoryTree, dimensions)),
                () -> 21
            );
        } catch (IllegalStateException ex) {
            // do nothing
        }
        assertThat(log.getMessages(TestLog.INFO))
            .contains("Procedure was blocked since minimum estimated memory (42 Bytes) exceeds current free memory (21 Bytes).");
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

        public TestTree(final String description, final MemoryRange range) {
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
