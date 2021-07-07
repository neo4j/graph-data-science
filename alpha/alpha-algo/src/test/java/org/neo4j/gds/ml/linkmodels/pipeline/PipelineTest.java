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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;

@GdlExtension
class PipelineTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String GRAPH =
        "(a:N {noise: 42, z: 13, array: [3.0,2.0]}), " +
        "(b:N {noise: 1337, z: 0, array: [1.0,1.0]}), " +
        "(c:N {noise: 42, z: 2, array: [8.0,2.3]}), " +
        "(d:N {noise: 42, z: 9, array: [0.1,91.0]}), " +

        "(a)-[:REL]->(b), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL]->(d)";

    @Inject
    Graph graph;


    // add several linkFeatureSteps + assert that linkFeatures computed correct
    @Test
    void singleLinkFeatureStep() {
        var pipeline = new Pipeline();

        pipeline.addLinkFeature(LinkFeatureStepFactory.HADAMARD.name(), Map.of("featureProperties", List.of("array")));

        var expected = HugeObjectArray.of(
            new double[]{3 * 1D, 2 * 1D}, // a-b
            new double[]{3 * 8, 2 * 2.3D}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D}, // a-d
            new double[]{3 * 1D, 2 * 1D}, // a-b
            new double[]{3 * 8, 2 * 2.3D}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D} // a-d
        );
        var actual = pipeline.computeLinkFeatures(graph);

        assertThat(actual.size()).isEqualTo(expected.size());

        for (long i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
        }
    }
}