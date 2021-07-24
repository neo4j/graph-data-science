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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class CosineLinkFeatureStepTest {
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
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    public void runCosineLinkFeatureStep() {

        var step = LinkFeatureStepFactory.create(
            "cosine",
            Map.of("featureProperties", List.of("noise", "z", "array"))
        );

        HugeObjectArray<double[]> linkFeatures = LinkFeatureExtractor.extractFeatures(graph, List.of(step));

        var delta = 0.0001D;

        var norm0 = Math.sqrt(42 * 42 + 13 * 13 + 3 * 3 + 2 * 2);
        var norm1 = Math.sqrt(1337 * 1337 + 0 * 0 + 1 * 1 + 1 * 1);
        var norm2 = Math.sqrt(42 * 42 + 2 * 2 + 8 * 8 + 2.3 * 2.3);
        var norm3 = Math.sqrt(42 * 42 + 9 * 9 + 0.1 * 0.1 + 91 * 91);

        assertEquals((42 * 1337 + 13 * 0D + 3 * 1D + 2 * 1D) / norm0 / norm1, linkFeatures.get(0)[0], delta);
        assertEquals((42 * 42 + 13 * 2 + 3 * 8 + 2 * 2.3D) / norm0 / norm2, linkFeatures.get(1)[0], delta);
        assertEquals((42 * 42 + 13 * 9 + 3 * 0.1D + 2 * 91.0D) / norm0 / norm3 , linkFeatures.get(2)[0], delta);
    }
}
