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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.memory.MemoryProc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class MemoryProcTest extends  BaseProgressTest {

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            MemoryProc.class,
            BaseProgressTestProc.class,
            GraphGenerateProc.class
        );

    }

    @Test
    void shouldReturnEmptyIfEmpty() {
        var rowCount = runQueryWithRowConsumer("CALL gds.listMemory", result->{} );
        assertThat(rowCount).isEqualTo(0L);
    }

    @Test
    void shouldReturnEmptySummary() {
        var rowCount = runQueryWithRowConsumer("alice",
            "CALL gds.listMemory.summary",
            resultRow -> {
            assertThat(resultRow.getString("user")).isEqualTo("alice");
                assertThat(resultRow.getNumber("totalGraphsMemory")).asInstanceOf(LONG).isEqualTo(0);
                assertThat(resultRow.getNumber("totalTasksMemory")).asInstanceOf(LONG).isEqualTo(0);
            });
        assertThat(rowCount).isEqualTo(1L);
    }

    @Test
    void shouldListGraphsAccordingly() {
       runQuery("alice", " CALL gds.graph.generate('random',10,1)");
        var rowCountAlice = runQueryWithRowConsumer("alice",
            "CALL gds.listMemory",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("alice");
                assertThat(resultRow.getString("name")).isEqualTo("random");
                assertThat(resultRow.getString("entity")).isEqualTo("graph");
                assertThat(resultRow.getNumber("memoryInBytes")).asInstanceOf(LONG).isGreaterThan(0);
            });
        assertThat(rowCountAlice).isEqualTo(1L);

        var rowCountBob = runQueryWithRowConsumer("bob",
            "CALL gds.listMemory",resultRow ->{}
            );

        assertThat(rowCountBob).isEqualTo(0L);
    }

    @Test
    void shouldSummarizeAccordingly() {
        runQuery("alice", " CALL gds.graph.generate('random',10,1)");
        var rowCountAlice = runQueryWithRowConsumer("alice",
            "CALL gds.listMemory",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("alice");
                assertThat(resultRow.getString("name")).isEqualTo("random");
                assertThat(resultRow.getString("entity")).isEqualTo("graph");
                assertThat(resultRow.getNumber("memoryInBytes")).asInstanceOf(LONG).isGreaterThan(0);

            });
        assertThat(rowCountAlice).isEqualTo(1L);

        var rowCountBob = runQueryWithRowConsumer("bob",
            "CALL gds.listMemory.summary",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("bob");
                assertThat(resultRow.getNumber("totalGraphsMemory")).asInstanceOf(LONG).isEqualTo(0);
                assertThat(resultRow.getNumber("totalTasksMemory")).asInstanceOf(LONG).isEqualTo(0);
            });

        assertThat(rowCountBob).isEqualTo(1L);
    }

    @Test
    void canListRunningTask() {
        runQuery("alice","CALL gds.test.pl('foo',true,false)");
        var rowCountAlice = runQueryWithRowConsumer("alice",
            "CALL gds.listMemory",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("alice");
                assertThat(resultRow.getString("entity")).isNotEqualTo("graph");
                assertThat(resultRow.getNumber("memoryInBytes")).asInstanceOf(LONG).isGreaterThan(0);
            });
        assertThat(rowCountAlice).isEqualTo(1L);

        var rowCountBob = runQueryWithRowConsumer("bob",
            "CALL gds.listMemory",resultRow ->{}
        );

        assertThat(rowCountBob).isEqualTo(0L);
    }

    @Test
    void canSummarizeRunningTask() {
        runQuery("alice","CALL gds.test.pl('foo',true,false)");

        var rowCountAlice = runQueryWithRowConsumer("alice",
            "CALL gds.listMemory.summary",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("alice");
                assertThat(resultRow.getNumber("totalGraphsMemory")).asInstanceOf(LONG).isEqualTo(0);
                assertThat(resultRow.getNumber("totalTasksMemory")).asInstanceOf(LONG).isGreaterThan(0);
            });
        assertThat(rowCountAlice).isEqualTo(1L);

        var rowCountBob = runQueryWithRowConsumer("bob",
            "CALL gds.listMemory.summary",
            resultRow -> {
                assertThat(resultRow.getString("user")).isEqualTo("bob");
                assertThat(resultRow.getNumber("totalGraphsMemory")).asInstanceOf(LONG).isEqualTo(0);
                assertThat(resultRow.getNumber("totalTasksMemory")).asInstanceOf(LONG).isEqualTo(0);
            });

        assertThat(rowCountBob).isEqualTo(1L);
    }

}
