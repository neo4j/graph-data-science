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
package org.neo4j.gds.kmeans;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KmeansTaskTest {

    @Test
    void memoryEstimation() {

        var estimation = KmeansTask.memoryEstimation(10, 128)
            .estimate(GraphDimensions.of(42, 1337), 4);

        var usage = estimation.memoryUsage();

        assertThat(usage.min).isEqualTo(5448L);
        assertThat(usage.max).isEqualTo(10568L);
    }
}
