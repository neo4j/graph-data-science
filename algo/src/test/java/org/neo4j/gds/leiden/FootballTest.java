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
package org.neo4j.gds.leiden;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.modularity.TestGraphs;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.ProcedureConstants.TOLERANCE_DEFAULT;

@GdlExtension
class FootballTest {

    @SuppressFBWarnings("HSC_HUGE_SHARED_STRING_CONSTANT")
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    public static final String GRAPH = TestGraphs.FOOTBALL_GRAPH;

    @Inject
    private TestGraph graph;

    @ParameterizedTest
    @ValueSource(longs = {99999, 25, 323, 405, 58, 61, 7, 8123, 94, 19})
    void leiden(long randomSeed) {
        var gamma = 1.0;
        Leiden leiden = new Leiden(
            graph,
            5,
            gamma,
            0.01,
            false,
            randomSeed,
            null,
            TOLERANCE_DEFAULT,
            1,
            ProgressTracker.NULL_TRACKER
        );
        var leidenResult = leiden.compute();
        var communities = leidenResult.communities();
        var communitiesMap = LongStream
            .range(0, graph.nodeCount())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(graph.toMappedNodeId(v))));

        assertThat(communitiesMap.values())
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a1",
                    "a3",
                    "a4",
                    "a5",
                    "a6",
                    "a9",
                    "a11",
                    "a12",
                    "a13",
                    "a38",
                    "a46",
                    "a47",
                    "a48",
                    "a53",
                    "a58",
                    "a59",
                    "a60",
                    "a61",
                    "a63",
                    "a64",
                    "a66",
                    "a67",
                    "a68",
                    "a69"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a10",
                    "a15",
                    "a42",
                    "a50",
                    "a71",
                    "a84",
                    "a85",
                    "a86",
                    "a87",
                    "a88",
                    "a89",
                    "a99",
                    "a101",
                    "a105",
                    "a106",
                    "a110"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a28",
                    "a39",
                    "a57",
                    "a70",
                    "a72",
                    "a73",
                    "a74",
                    "a75",
                    "a76",
                    "a78",
                    "a79",
                    "a80",
                    "a91",
                    "a92",
                    "a93"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a25",
                    "a33",
                    "a34",
                    "a37",
                    "a40",
                    "a41",
                    "a43",
                    "a44",
                    "a45",
                    "a49",
                    "a51",
                    "a52"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a24",
                    "a26",
                    "a27",
                    "a29",
                    "a30",
                    "a31",
                    "a32",
                    "a35",
                    "a36",
                    "a54",
                    "a55"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a8",
                    "a20",
                    "a56",
                    "a65",
                    "a90",
                    "a94",
                    "a95",
                    "a97",
                    "a98",
                    "a107"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a2",
                    "a7",
                    "a14",
                    "a16",
                    "a17",
                    "a19",
                    "a21",
                    "a22",
                    "a23"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a62",
                    "a77",
                    "a82",
                    "a83",
                    "a102",
                    "a104",
                    "a109",
                    "a114",
                    "a115"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a18",
                    "a81",
                    "a96",
                    "a100",
                    "a103",
                    "a108",
                    "a111",
                    "a112",
                    "a113"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder("a0")
            );
        assertThat(leidenResult.modularity()).isCloseTo(0.60440, Offset.offset(1e-3));
    }

}
