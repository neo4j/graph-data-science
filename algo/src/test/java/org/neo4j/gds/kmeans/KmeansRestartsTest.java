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
package org.neo4j.gds.kmeans;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class KmeansRestartsTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a {  kmeans: [1.0]} )" +
        "  (b {  kmeans: [1.1]} )" +
        "  (c {  kmeans: [100.0]} )";


    @Inject
    private Graph graph;


    @Inject
    private IdFunction idFunction;

    @Test
    void shouldChangeWithRestarts() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .maxIterations(1)
            .randomSeed(11L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var result = kmeans.compute().communities();
        assertThat(result.get(0)).isNotEqualTo(result.get(1));
        assertThat(result.get(2)).isEqualTo(result.get(1));

        var kmeansConfig2 = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(11L)
            .maxIterations(1)
            .numberOfRestarts(2)
            .k(2)
            .build();

        var kmeans2 = Kmeans.createKmeans(graph, kmeansConfig2, kmeansContext);
        var result2 = kmeans2.compute().communities();
        assertThat(result2.get(2)).isNotEqualTo(result2.get(1));
        assertThat(result2.get(0)).isEqualTo(result2.get(1));


    }
    
}
