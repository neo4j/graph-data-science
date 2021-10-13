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
package org.neo4j.gds.core.loading.nodeproperties;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.nodeMapping;

public class LongNodePropertiesBuilderTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#usePartitionedIndexScan")
    void singleLabelAssignmentWithNonDirectMapping(TestMethodRunner runner) {
        int nodeCount = 10;
        var defaultValue = DefaultValue.of(10L);

        var originalIds = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            originalIds[i] = i * 42L;
        }

        var nodeMapping = nodeMapping(originalIds);

        runner.run(() -> {
            var builder = LongNodePropertiesBuilder.of(
                nodeCount,
                defaultValue,
                AllocationTracker.empty(),
                ConcurrencyConfig.DEFAULT_CONCURRENCY
            );

            for (int i = 0; i < nodeCount; i++) {
                builder.set(i, originalIds[i], i * 1337L);
            }

            var longNodeProperties = builder.buildInner(nodeMapping);

            for (int i = 0; i < nodeCount; i++) {
                assertThat(longNodeProperties.get(i)).isEqualTo(i * 1337L);
            }
        });
    }
}
