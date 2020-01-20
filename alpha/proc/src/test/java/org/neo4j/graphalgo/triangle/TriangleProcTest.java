/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.triangle.TriangleConfig;
import org.neo4j.graphalgo.impl.triangle.TriangleCountConfig;
import org.neo4j.graphalgo.impl.triangle.TriangleStream;

import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

class TriangleProcTest extends TriangleBaseProcTest<TriangleStream, Stream<TriangleStream.Result>, TriangleConfig> {

    @Override
    TriangleBaseProc<TriangleStream, Stream<TriangleStream.Result>, TriangleConfig> newInstance() {
        return new TriangleProc();
    }

    @Override
    TriangleConfig newConfig() {
        return TriangleConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            CypherMapWrapper.empty()
        );
    }

    private static String[] idToName;

    @BeforeEach
    void setup() throws Exception {
        super.setup();
        idToName = new String[9];

        QueryRunner.runInTransaction(db, () -> {
            for (int i = 0; i < 9; i++) {
                final String name = (String) db.getNodeById(i).getProperty("name");
                idToName[i] = name;
            }
        });
    }

    private static int idsum(String... names) {
        int sum = 0;
        for (int i = 0; i < idToName.length; i++) {
            for (String name : names) {
                if (idToName[i].equals(name)) {
                    sum += i;
                }
            }
        }
        return sum;
    }

    @Test
    void testStreaming() {
        HashSet<Integer> sums = new HashSet<>();
        TripleConsumer consumer = (a, b, c) -> sums.add(idsum(a, b, c));
        String query = GdsCypher
            .call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds", "alpha", "triangle")
            .streamMode()
            .yields();
        runQueryWithRowConsumer(query, row -> {
            long nodeA = row.getNumber("nodeA").longValue();
            long nodeB = row.getNumber("nodeB").longValue();
            long nodeC = row.getNumber("nodeC").longValue();
            consumer.consume(idToName[(int) nodeA], idToName[(int) nodeB], idToName[(int) nodeC]);
        });

        assertThat(sums, containsInAnyOrder(0 + 1 + 2, 3 + 4 + 5, 6 + 7 + 8));
    }

    interface TripleConsumer {
        void consume(String nodeA, String nodeB, String nodeC);
    }
}
