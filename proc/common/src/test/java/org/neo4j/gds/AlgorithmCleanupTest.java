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
package org.neo4j.gds;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.test.TestProc;
import org.neo4j.logging.NullLog;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlgorithmCleanupTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class);
        runQuery("CREATE (n)-[:REL]->(m)");
        runQuery("CALL gds.graph.create('g', '*', '*')");
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void cleanupTaskRegistryUnderRegularExecution() {
        var taskRegistry = new TestTaskRegistry();

        var proc = new TestProc();
        proc.taskRegistry = taskRegistry;
        proc.api = db;
        proc.log = NullLog.getInstance();
        Map<String, Object> config = Map.of("writeProperty", "test");

        assertThatCode(() -> proc.stats("g", config)).doesNotThrowAnyException();
        assertThat(taskRegistry.unregisterTaskCalls()).isEqualTo(1);
    }

    @Test
    void cleanupTaskRegistryWhenTheAlgorithmFails() {
        var taskRegistry = new TestTaskRegistry();

        var proc = new TestProc();
        proc.taskRegistry = taskRegistry;
        proc.api = db;
        proc.log = NullLog.getInstance();
        Map<String, Object> config = Map.of("writeProperty", "test", "throwInCompute", true);

        assertThatThrownBy(() -> proc.stats("g", config)).isNotNull();
        assertThat(taskRegistry.unregisterTaskCalls()).isEqualTo(1);
    }
}
