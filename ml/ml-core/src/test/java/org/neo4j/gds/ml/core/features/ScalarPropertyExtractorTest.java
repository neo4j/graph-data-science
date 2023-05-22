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
package org.neo4j.gds.ml.core.features;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class ScalarPropertyExtractorTest {

    @GdlGraph
    private static final String GDL = "(a {q: 1.0d}), (b {q: NaN}), (c)";

    @Inject
    private TestGraph graph;

    @Test
    void extracts() {
        var extractor = new ScalarPropertyExtractor(graph, "q");

        assertThat(extractor.dimension()).isEqualTo(1);

        // succeeds
        assertThat(extractor.extract(graph.toMappedNodeId("a"))).isEqualTo(1D);

        // failure conditions
        assertThatThrownBy(() -> extractor.extract(graph.toMappedNodeId("b")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Node with ID `" + graph.toOriginalNodeId("b") + "` has invalid feature property value `NaN` for property `q`");

        // missing is default NaN
        assertThatThrownBy(() -> extractor.extract(graph.toMappedNodeId("c")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Node with ID `" + graph.toOriginalNodeId("c") + "` has invalid feature property value `NaN` for property `q`");
    }

}
