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
package org.neo4j.gds.core.io.file.csv;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.io.file.csv.estimation.CsvExportEstimation;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class CsvExportEstimationTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (a:A:B { prop1: 0, prop2: 42, prop3: [1L, 3L, 3L, 7L]})" +
        ", (b:A:B { prop1: 1, prop2: 43})" +
        ", (c:A:C { prop1: 2, prop2: 44, prop3: [1L, 9L, 8L, 4L] })" +
        ", (d:B { prop1: 3 })" +
        ", (a)-[:REL1 { prop1: 0, prop2: 42 }]->(a)" +
        ", (a)-[:REL1 { prop1: 1, prop2: 43 }]->(b)" +
        ", (b)-[:REL1 { prop1: 2, prop2: 44 }]->(a)" +
        ", (b)-[:REL2 { prop3: 3, prop4: 45 }]->(c)" +
        ", (c)-[:REL2 { prop3: 4, prop4: 46 }]->(d)" +
        ", (d)-[:REL2 { prop3: 5, prop4: 47 }]->(a)";

    @Inject
    public GraphStore graphStore;

    @Test
    void estimate() {
        MemoryRange estimation = CsvExportEstimation
            .estimate(graphStore, 1)
            .estimate(GraphDimensions.of(0), 1)
            .memoryUsage();

        // The actual value is 40K
        assertThat(estimation.max).isBetween(100L, 200L);
    }
}
