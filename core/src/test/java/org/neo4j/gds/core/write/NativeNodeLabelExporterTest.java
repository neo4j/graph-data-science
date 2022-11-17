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
package org.neo4j.gds.core.write;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;

import static org.assertj.core.api.Assertions.assertThat;

@Neo4jGraphExtension
class NativeNodeLabelExporterTest extends BaseTest {

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:Node {prop1: 1.0, prop2: 42.0})" +
        ", (n2:Node {prop1: 2.0, prop2: 42.0})" +
        ", (n3:Node {prop1: 3.0, prop2: 42.0})" +
        ", (n4:AnotherNode {prop1: 3.0, prop2: 42.0})";

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void exportLabel(int concurrency) {
        Graph graph = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graph();

        var exporter = new NativeNodeLabelExporter(
            TestSupport.fullAccessTransaction(db),
            graph.nodeCount(),
            graph::toOriginalNodeId,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            concurrency,
            Pools.DEFAULT
        );

        exporter.write("GeneratedLabel");

        assertThat(exporter.nodeLabelsWritten()).isEqualTo(4);

        runQueryWithRowConsumer("MATCH (n) RETURN labels(n) AS labels", row -> {
            assertThat(row.get("labels"))
                .asList()
                .contains("GeneratedLabel");
        });
    }
}
