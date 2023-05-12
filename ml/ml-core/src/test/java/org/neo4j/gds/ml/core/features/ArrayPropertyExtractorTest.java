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

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class ArrayPropertyExtractorTest {

    @GdlGraph
    private static final String GDL = "({q: [1.0d, 2.0d]}), ({q: [4.0d, 6.0d, 4.0d]}), (), ({q: [-2.0d, NaN]})";

    @Inject
    private TestGraph graph;

    @Test
    void extracts() {
        var extractor = new ArrayPropertyExtractor(2, graph, "q");

        assertThat(extractor.dimension()).isEqualTo(2);

        // succeeds
        assertThat(extractor.extract(0)).containsExactly(1, 2);

        // failure conditions
        assertThatThrownBy(() -> extractor.extract(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The property `q` contains arrays of differing lengths `3` and `2`");

        assertThatThrownBy(() -> extractor.extract(2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(String.format(
                Locale.US,
                "Missing node property for property key `q` on node with id `%s`",
                graph.toOriginalNodeId(2)
            ));

        assertThatThrownBy(() -> extractor.extract(3))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(String.format(
                Locale.US,
                "Node with ID `%s` has invalid feature property value NaN for property `q`",
                graph.toOriginalNodeId(3)
            ));
    }

}
