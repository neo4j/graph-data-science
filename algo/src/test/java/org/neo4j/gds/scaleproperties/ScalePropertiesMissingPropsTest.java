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
package org.neo4j.gds.scaleproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.scaling.Max;
import org.neo4j.gds.scaling.StdScore;

import java.util.List;

import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@GdlExtension
class ScalePropertiesMissingPropsTest {

    @GdlGraph
    static String GDL =
        "(a:A {a: 1.1D, b: 20, c: 50, arrayOn4: [20L], arrayOn1: [1.0,2.0]}), " +
        "(b:A {         b: 21, c: 51, arrayOn4: [21L]                     }), " +
        "(c:A {                c: 52, arrayOn4: [22L]                     }), " +
        "(d:A {                       arrayOn4: [23L]                     }), " +
        "(e:A {         b: 25                                             })";

    @Inject
    private TestGraph graph;

    @Test
    void partialArrays() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("arrayOn4", "arrayOn1"))
            .scaler(Max.buildFrom(CypherMapWrapper.empty()))
            .build();
        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, DefaultPool.INSTANCE);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{.869, 1D,  1D }, resultProperties[(int) graph.toMappedNodeId("a")], 1e-3);
        assertArrayEquals(new double[]{.913, NaN, NaN}, resultProperties[(int) graph.toMappedNodeId("b")], 1e-3);
        assertArrayEquals(new double[]{.956, NaN, NaN}, resultProperties[(int) graph.toMappedNodeId("c")], 1e-3);
        assertArrayEquals(new double[]{1D,   NaN, NaN}, resultProperties[(int) graph.toMappedNodeId("d")], 1e-3);
        assertArrayEquals(new double[]{NaN,  NaN, NaN}, resultProperties[(int) graph.toMappedNodeId("e")], 1e-3);
    }

    @Test
    void testMissingScalar() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a", "b", "c"))
            .scaler(StdScore.buildFrom(CypherMapWrapper.empty()))
            .build();
        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, DefaultPool.INSTANCE);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{0D,  -.926, -1.225}, resultProperties[(int) graph.toMappedNodeId("a")], 1e-3);
        assertArrayEquals(new double[]{0D, -.463, 0D     }, resultProperties[(int) graph.toMappedNodeId("b")], 1e-3);
        assertArrayEquals(new double[]{0D, NaN,   1.225  }, resultProperties[(int) graph.toMappedNodeId("c")], 1e-3);
        assertArrayEquals(new double[]{0D, NaN,   NaN    }, resultProperties[(int) graph.toMappedNodeId("d")], 1e-3);
        assertArrayEquals(new double[]{0D, 1.389, NaN    }, resultProperties[(int) graph.toMappedNodeId("e")], 1e-3);
    }

}
