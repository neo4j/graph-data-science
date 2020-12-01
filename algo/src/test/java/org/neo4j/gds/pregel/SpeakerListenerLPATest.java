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
package org.neo4j.gds.pregel;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Arrays;

@GdlExtension
class SpeakerListenerLPATest {

    @GdlGraph
    private static final String GDL =
        "(a), (b), (c), (d), (e) " +

        "(a)-->(b), " +
        "(a)-->(b), " +
        "(a)-->(c), " +
        "(b)-->(a), " +
        "(c)-->(d), " +
        "(c)-->(c), ";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void unweighted() {
        var config = ImmutableSpeakerListenerLPAConfig.builder().maxIterations(5).build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new SpeakerListenerLPA(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        var labels = pregelJob.run().nodeValues().longArrayProperties("labels");

        System.out.println("labels = " + labels);
        System.out.println("(\"a\")) = " + Arrays.toString(labels.get(idFunction.of("a"))));
        System.out.println("(\"b\")) = " + Arrays.toString(labels.get(idFunction.of("b"))));
        System.out.println("(\"c\")) = " + Arrays.toString(labels.get(idFunction.of("c"))));
        System.out.println("(\"d\")) = " + Arrays.toString(labels.get(idFunction.of("d"))));
        System.out.println("(\"e\")) = " + Arrays.toString(labels.get(idFunction.of("e"))));
    }

}