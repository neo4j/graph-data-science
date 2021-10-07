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
package org.neo4j.gds.catalog;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;

import java.util.concurrent.Phaser;

@ExtendWith(SoftAssertionsExtension.class)
class TransactionsWhileProjectingIntegrationTest extends BaseProcTest {

    private static final String CREATE_A_NODE = "CREATE (a:Node)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(GraphCreateProc.class);
        runQuery(CREATE_A_NODE);
    }

    @Test
    void concurrentUpdatesShouldNotBreakTheGraphProjection(SoftAssertions softly) throws InterruptedException {

        var phaser = new Phaser(2);

        var thread1 = new Thread(() -> {
            // wait for both threads to arrive, but deregister this thread
            // there is only one party active after that (thread2)
            phaser.awaitAdvance(phaser.arriveAndDeregister());

            while (!phaser.isTerminated()) {
                runQuery(CREATE_A_NODE);
            }
        });

        var thread2 = new Thread(() -> {
            // wait for both phasers to arrive, keep this one registered
            phaser.arriveAndAwaitAdvance();

            try {
                runQuery(
                    GdsCypher.call()
                        .withNodeLabel("Node")
                        .withAnyRelationshipType()
                        .graphCreate("g")
                        .yields()
                );
            } finally {
                // before we are finished we deregister this thread as well
                // the phaser now has all parties de-registered
                // it can now transition into terminated and stop thread1
                phaser.arriveAndDeregister();
            }
        });

        thread2.setUncaughtExceptionHandler((t, e) -> softly.fail("graph.create failed", e));

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
    }
}
