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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.Neo4jVersion;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.test.TestAlgorithm;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

public class AllocationTrackingAlgorithmTest extends AlgoTestBase {

    private static final String EXCEPTION_NAME = "MemoryLimitExceededException";
    private static final long MEMORY_LIMIT = 1024 * 1024 + 1;

    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.impermanent();
        builder.noOpSystemGraphInitializer();
        builder.setConfig(Settings.memoryTransactionMaxSize(), MEMORY_LIMIT);
    }

    @Test
    void shouldThrowWhenOverAllocating() {
        Assumptions.assumeFalse(is40(), "There is no KernelTracker in 4.0");

        Graph graph = fromGdl("()-->()");

        GdsFeatureToggles.USE_KERNEL_TRACKER.enableAndRun(
            () -> {
                TestAlgorithm algorithm;
                AllocationTracker tracker;
                try (Transaction tx = db.beginTx()) {
                    var ktx = ((InternalTransaction) tx).kernelTransaction();
                    var memoryTrackerProxy = Neo4jProxy.memoryTrackerProxy(ktx);
                    tracker = AllocationTracker.create(memoryTrackerProxy);
                    algorithm = new TestAlgorithm(
                        graph,
                        tracker,
                        MEMORY_LIMIT,
                        new TestLog(),
                        EmptyProgressEventTracker.INSTANCE,
                        false
                    );
                    tx.commit();
                }
                var exception = rootCause(assertThrows(Exception.class, algorithm::compute));
                assertThat(exception.getClass().getSimpleName()).isEqualTo(EXCEPTION_NAME);
                assertThat(exception).hasMessageStartingWith("The allocation of an extra");
            }
        );
    }

    private boolean is40() {
        return GraphDatabaseApiProxy.neo4jVersion() == Neo4jVersion.V_4_0;
    }
}
