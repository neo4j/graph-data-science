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
package org.neo4j.gds.assertions;

import org.assertj.core.api.AbstractAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

public final class MemoryEstimationAssert extends AbstractAssert<MemoryEstimationAssert, MemoryEstimation> {

    private MemoryEstimationAssert(MemoryEstimation memoryEstimation) {
        super(memoryEstimation, MemoryEstimationAssert.class);
    }

    public static MemoryEstimationAssert assertThat(MemoryEstimation actual) {
        return new MemoryEstimationAssert(actual);
    }

    public MemoryEstimationAssert hasDescription(String expectedDescription) {
        isNotNull();

        if (!actual.description().equals(expectedDescription)) {
            failWithMessage(
                "Expected description `%s` to be equal to `%s`",
                expectedDescription,
                actual.description()
            );
        }

        return this;
    }

    public MemoryRangeAssert memoryRange(long nodeCount, long relationshipCount, int concurrency) {
        isNotNull();
        var memoryRange = actual.estimate(GraphDimensions.of(nodeCount, relationshipCount), concurrency).memoryUsage();
        return MemoryRangeAssert.assertThat(memoryRange);
    }

    public MemoryRangeAssert memoryRange(long nodeCount, int concurrency) {
        return memoryRange(nodeCount, 0, concurrency);
    }
}
