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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;

@GdlExtension
class LinkFeatureExtractorTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GRAPH = "CREATE " +
                                        "(a:N {noise: 42, z: 13, array: [3.0,2.0]}), " +
                                        "(b:N {noise: 1337, z: 0, array: [1.0,1.0]}), " +
                                        "(c:N {noise: 42, z: 2, array: [8.0,2.3]}), " +
                                        "(d:N {noise: 42, z: 9, array: [0.1,91.0]}), " +

                                        "(a)-[:REL]->(b), " +
                                        "(a)-[:REL]->(c), " +
                                        "(a)-[:REL]->(d)";

    @Inject
    Graph graph;

    @Test
    void singleLinkFeatureStep() {
        var actual = LinkFeatureExtractor.extractFeatures(
            graph,
            List.of(new HadamardFeatureStep(List.of("array"))),
            1,
            ProgressTracker.NULL_TRACKER
        );

        var expected = HugeObjectArray.of(
            new double[]{3 * 1D, 2 * 1D}, // a-b
            new double[]{3 * 8, 2 * 2.3D}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D}, // a-d
            new double[]{3 * 1D, 2 * 1D}, // a-b
            new double[]{3 * 8, 2 * 2.3D}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D} // a-d
        );

        assertThat(actual.size()).isEqualTo(expected.size());

        for (long i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
        }
    }

    @Test
    void multipleLinkFeatureStep() {
        var actual = LinkFeatureExtractor.extractFeatures(
            graph,
            List.of(
                new HadamardFeatureStep(List.of("array")),
                new CosineFeatureStep(List.of("noise", "z"))
            ),
            1,
            ProgressTracker.NULL_TRACKER
        );

        var normA = Math.sqrt(42 * 42 + 13 * 13);
        var normB = Math.sqrt(1337 * 1337 + 0 * 0);
        var normC = Math.sqrt(42 * 42 + 2 * 2);
        var normD = Math.sqrt(42 * 42 + 9 * 9);

        var expected = HugeObjectArray.of(
            new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
            new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD}, // a-d
            new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
            new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
            new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD} // a-d
        );

        assertThat(actual.size()).isEqualTo(expected.size());

        for (long i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
        }
    }
}
