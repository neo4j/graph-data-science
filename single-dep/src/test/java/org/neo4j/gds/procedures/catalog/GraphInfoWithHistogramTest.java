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
package org.neo4j.gds.procedures.catalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

class GraphInfoWithHistogramTest {
    @Test
    void shouldConstructWithDegreeDistribution() {
        var creationTime = ZonedDateTime.of(1969, 7, 20, 20, 17, 40, 0, ZoneId.of("UTC"));
        var modificationTime = ZonedDateTime.of(1963, 8, 28, 17, 0, 0, 0, ZoneId.of("GMT-5"));
        var graphProjectConfig = GraphProjectFromStoreConfig.of(
            "some user",
            "some graph",
            "A",
            "REL",
            CypherMapWrapper.create(
                Map.of(
                    "creationTime", creationTime,
                    "jobId", "some job"
                )
            )
        );
        var graphStore = new DummyGraphStore(modificationTime);
        Map<String, Object> degreeDistribution = Map.of(
            "min", 5L,
            "mean", 5.0D,
            "max", 5L,
            "p50", 5L,
            "p75", 5L,
            "p90", 5L,
            "p95", 5L,
            "p99", 5L,
            "p999", 5L
        );
        var graphInfoWithHistogram = GraphInfoWithHistogram.of(
            graphProjectConfig,
            graphStore,
            degreeDistribution,
            false
        );

        assertThat(graphInfoWithHistogram.creationTime).isEqualTo(creationTime);
        assertThat(graphInfoWithHistogram.configuration).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "creationTime", creationTime,
                "jobId", "some job",
                "logProgress", true,
                "nodeProjection", Map.of(
                    "A", Map.of(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "nodeProperties", emptyMap(),
                "readConcurrency", 4,
                "relationshipProjection", Map.of(
                    "REL", Map.of(
                        "type", "REL",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "indexInverse", false,
                        "properties", emptyMap()
                    )
                ),
                "relationshipProperties", emptyMap(),
                "sudo", false,
                "validateRelationships", false
            )
        );
        assertThat(graphInfoWithHistogram.database).isEqualTo("some database");
        assertThat(graphInfoWithHistogram.degreeDistribution).containsExactlyInAnyOrderEntriesOf(Map.of(
            "min", 5L,
            "mean", 5.0D,
            "max", 5L,
            "p50", 5L,
            "p75", 5L,
            "p90", 5L,
            "p95", 5L,
            "p99", 5L,
            "p999", 5L
        ));
        assertThat(graphInfoWithHistogram.density).isEqualTo(0.5);
        assertThat(graphInfoWithHistogram.graphName).isEqualTo("some graph");
        assertThat(graphInfoWithHistogram.memoryUsage).isEqualTo("");
        assertThat(graphInfoWithHistogram.modificationTime).isEqualTo(modificationTime);
        assertThat(graphInfoWithHistogram.nodeCount).isEqualTo(2L);
        assertThat(graphInfoWithHistogram.relationshipCount).isEqualTo(1L);
        assertThat(graphInfoWithHistogram.schema).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "nodes", Map.of("A", Map.of()),
                "relationships", Map.of("REL", Map.of()),
                "graphProperties", Map.of()
            )
        );
        assertThat(graphInfoWithHistogram.schemaWithOrientation).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "nodes", Map.of("A", Map.of()),
                "relationships", Map.of("REL", Map.of("direction", "DIRECTED", "properties", Map.of())),
                "graphProperties", Map.of()
            )
        );
        assertThat(graphInfoWithHistogram.sizeInBytes).isEqualTo(-1L);
    }
}
