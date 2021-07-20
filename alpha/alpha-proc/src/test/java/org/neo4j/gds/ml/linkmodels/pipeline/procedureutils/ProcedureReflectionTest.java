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
package org.neo4j.gds.ml.linkmodels.pipeline.procedureutils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.graphalgo.louvain.LouvainMutateProc;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class ProcedureReflectionTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (a)-[:R]->(b)" +
        ", (b)-[:R]->(c)" +
        ", (c)-[:R]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .loadEverything()
            .graphCreate("g")
            .yields();

        runQuery(createQuery);
    }

    @Test
    void testInvokeProc() {
        applyOnProcedure(proc -> {
            ProcedureReflection.INSTANCE.invokeProc(proc, "pageRank.mutate", "g", Map.of("mutateProperty", "foo"));
        });
        String streamQuery = "CALL gds.graph.streamNodeProperties('g', ['foo'])";
        runQueryWithResultConsumer(streamQuery, result -> {
            for (int i = 0; i < 3; i++) {
                assertThat(result.next().get("propertyValue")).isEqualTo(0.9612404689154856);
            }
            assertFalse(result.hasNext());
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"boogaloo", "gds.graph.create", "ageRank"})
    void shouldFailOnInvalidProc(String proc) {
        assertThatThrownBy(() -> ProcedureReflection.INSTANCE.findProcedureMethod(proc))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(formatWithLocale("Invalid procedure name `%s` for pipelining.", proc));
    }

    @Test
    void shouldFailOnNonUniqueMatch() {
        assertThatThrownBy(() -> ProcedureReflection.INSTANCE.findProcedureMethod("dijkstra"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith(
                "Ambiguous procedure name `dijkstra`. Found matching procedures gds.shortestPath.dijkstra.mutate, gds.allShortestPaths.dijkstra.mutate.");
    }

    void applyOnProcedure(Consumer<? super AlgoBaseProc<?, ?, ?>> func) {
        try (GraphDatabaseApiProxy.Transactions transactions = newKernelTransaction(db)) {
            // TODO: replace with for example LinkPrediction.train procedure (although maybe not worth it)
            AlgoBaseProc<?, ?, ?> proc = new LouvainMutateProc(); // any proc really, just highjacking state

            proc.procedureTransaction = transactions.tx();
            proc.transaction = transactions.ktx();
            proc.api = db;
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = new TestLog();
            proc.progressTracker = EmptyProgressEventTracker.INSTANCE;

            func.accept(proc);
        }
    }
}
