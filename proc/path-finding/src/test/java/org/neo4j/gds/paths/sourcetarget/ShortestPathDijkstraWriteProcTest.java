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
package org.neo4j.gds.paths.sourcetarget;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NonReleasingTaskRegistry;
import org.neo4j.gds.TestLogProvider;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.gds.config.WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.paths.PathTestUtil.validationQuery;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ShortestPathDijkstraWriteProcTest extends ShortestPathDijkstraProcTest<ShortestPathDijkstraWriteConfig> {

    TestLog testLog;

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig, ?>> getProcedureClazz() {
        return ShortestPathDijkstraWriteProc.class;
    }

    @Override
    public ShortestPathDijkstraWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathDijkstraWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(WRITE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = Neo4jProxy.testLog();
        builder.setUserLogProvider(new TestLogProvider(testLog));
    }

    @Test
    void testWrite() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", true)
            .addParameter("writeCosts", true)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        assertCypherResult(validationQuery(idA), List.of(Map.of("totalCost", 20.0D, "nodeIds", ids0, "costs", costs0)));
    }

    @ParameterizedTest
    @CsvSource(value = {"true,false", "false,true", "false,false"})
    void testWriteFlags(boolean writeNodeIds, boolean writeCosts) {
        var relationshipWeightProperty = "cost";

        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("writeRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .addParameter("writeNodeIds", writeNodeIds)
            .addParameter("writeCosts", writeCosts)
            .yields();

        runQuery(query);

        var validationQuery = "MATCH ()-[r:%s]->() RETURN r.nodeIds AS nodeIds, r.costs AS costs";
        var rowCount = new MutableInt(0);
        runQueryWithRowConsumer(formatWithLocale(validationQuery, WRITE_RELATIONSHIP_TYPE), row -> {
            rowCount.increment();
            var nodeIds = row.get("nodeIds");
            var costs = row.get("costs");

            if (writeNodeIds) {
                assertNotNull(nodeIds);
            } else {
                assertNull(nodeIds);
            }

            if (writeCosts) {
                assertNotNull(costs);
            } else {
                assertNull(costs);
            }
        });
        assertEquals(1, rowCount.getValue());
    }

    @Test
    void testLazyComputationLoggingFinishes() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .writeMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "BAR")
            .yields();

        runQuery(query);

        var messages = testLog.getMessages(TestLog.INFO);
        assertThat(messages.get(messages.size() - 1)).contains(":: Finished");
    }

    @Test
    void testProgressTracking() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        applyOnProcedure(proc -> {
            var pathProc = ((ShortestPathDijkstraWriteProc) proc);

            var taskStore = new GlobalTaskStore();

            pathProc.taskRegistryFactory = jobId -> new NonReleasingTaskRegistry(new TaskRegistry(getUsername(), taskStore, jobId));

            pathProc.write(
                "graph",
                Map.of(
                    "sourceNode", config.sourceNode(),
                    "targetNode", config.targetNode(),
                    "relationshipWeightProperty", "cost",
                    "writeRelationshipType", "BAR"
                )
            );

            assertThat(taskStore.query().map(TaskStore.UserTask::task).map(Task::description)).containsExactlyInAnyOrder(
                "Dijkstra",
                "Write shortest Paths :: WriteRelationshipStream"
            );
        });
    }
}
