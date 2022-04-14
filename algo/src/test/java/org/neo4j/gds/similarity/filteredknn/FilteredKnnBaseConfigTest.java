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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class FilteredKnnBaseConfigTest {

    @GdlGraph
    static final String GDL = "CREATE (a), (b)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @Test
    void shouldAcceptValidSourceNodeFilter() {
        new FilteredKnnBaseConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "nodeProperties", List.of("dummy"),
                    "sourceNodeFilter", List.of(idFunction.of("a"))
                )
            )
        ).validateSourceNodeFilter(
            graphStore,
            List.of(),
            List.of()
        );
    }

    @Test
    void shouldRejectOutOfRangeSourceNodeFilter() {
        var outOfRangeNode = -1L;
        assertThatThrownBy(
            () -> new FilteredKnnBaseConfigImpl(
                CypherMapWrapper.create(
                    Map.of(
                        "nodeProperties", List.of("dummy"),
                        "sourceNodeFilter", List.of(outOfRangeNode)
                    )
                )
            )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Value for `sourceNodeFilter` was `" + outOfRangeNode + "`, but must be within the range [0, 9223372036854775807].");
    }

    @Test
    void shouldRejectSourceNodeFilterWithMissingNode() {
        //noinspection OptionalGetWithoutIsPresent
        var missingNode = new Random()
            .longs(
                0,
                4_294_967_295L // a large-ish number that still fits in our id maps (Math.pow(2, 32) - 1)
            ).filter(l -> !graphStore.nodes().contains(l))
            .limit(1)
            .findFirst()
            .getAsLong();

        assertThatThrownBy(
            () -> new FilteredKnnBaseConfigImpl(
                CypherMapWrapper.create(
                    Map.of(
                        "nodeProperties", List.of("dummy"),
                        "sourceNodeFilter", List.of(idFunction.of("a"), missingNode) // one existing, one missing
                    )
                )
            ).validateSourceNodeFilter(
                graphStore,
                List.of(),
                List.of()
            )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid configuration value 'sourceNodeFilter', the following nodes are missing from the graph: [" + missingNode + "]");
    }
}
