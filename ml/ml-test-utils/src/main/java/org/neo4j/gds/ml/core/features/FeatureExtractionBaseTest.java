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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@GdlExtension
public abstract class FeatureExtractionBaseTest {

    @GdlGraph(graphNamePrefix = "valid")
    private static final String VALID =
        "CREATE " +
        "  (n1:N {a: [2.0, 1.0], b: 1.2})" +
        ", (n2:N {a: [1.3, 1.0], b: 0.5})" +
        ", (n3:N {a: [0.0, 1.0], b: 2.8})" +
        ", (n4:N {a: [1.0, 1.0], b: 0.9})";

    @GdlGraph(graphNamePrefix = "missingArray")
    private static final String MISSING_ARRAY =
        "CREATE " +
        "  (n1:N {b: 1.2})" +
        ", (n2:N {a: [1.3, 1.0], b: 0.5})" +
        ", (n3:N {a: [0.0, 1.0], b: 2.8})" +
        ", (n4:N {a: [1.0, 1.0], b: 0.9})";

    @GdlGraph(graphNamePrefix = "missingArray2")
    private static final String MISSING_ARRAY2 =
        "CREATE " +
        "  (n1:N {a: [1.3, 1.0], b: 1.2})" +
        ", (n2:N {b: 0.5})" +
        ", (n3:N {a: [0.0, 1.0], b: 2.8})" +
        ", (n4:N {a: [1.0, 1.0], b: 0.9})";

    @GdlGraph(graphNamePrefix = "missingScalar")
    private static final String MISSING_SCALAR =
        "CREATE " +
        "  (n1:N {a: [2.0, 1.0]})" +
        ", (n2:N {a: [1.3, 1.0], b: 0.5})" +
        ", (n3:N {a: [0.0, 1.0], b: 2.8})" +
        ", (n4:N {a: [1.0, 1.0], b: 0.9})";

    @GdlGraph(graphNamePrefix = "invalid")
    private static final String INVALID =
        "CREATE " +
        "  (n1:N {a: [2.0, 1.0], b: 1.2})" +
        ", (n2:N {a: [1.3], b: 0.5})" +
        ", (n3:N {a: [0.0, 1.0], b: 2.8})" +
        ", (n4:N {a: [1.0, 1.0], b: 0.9})";

    @Inject
    protected Graph validGraph;

    @Inject
    protected TestGraph missingArrayGraph;

    @Inject
    protected TestGraph missingArray2Graph;

    @Inject
    protected TestGraph missingScalarGraph;

    @Inject
    protected TestGraph invalidGraph;

    public abstract void makeExtractions(Graph graph);

    @Test
    void shouldFailOnMissingScalarProperty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> makeExtractions(missingScalarGraph))
            .withMessageContaining(String.format(
                Locale.US,
                "Node with ID `%d` has invalid feature property value `NaN` for property `b`",
                missingScalarGraph.toOriginalNodeId("n1")
            ));
    }

    @Test
    void shouldFailOnMissingArrayPropertyFirstNode() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> makeExtractions(missingArrayGraph))
            .withMessageContaining(
                String.format(
                    Locale.US,
                    "Missing node property for property key `a` on node with id `%d`.",
                    missingArrayGraph.toOriginalNodeId("n1")
                )
            );
    }

    @Test
    void shouldFailOnMissingArrayPropertySecondNode() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> makeExtractions(missingArray2Graph))
            .withMessageContaining(
                String.format(
                    Locale.US,
                    "Missing node property for property key `a` on node with id `%d`.",
                    missingArray2Graph.toOriginalNodeId("n2")
                )

            );
    }

    @Test
    void shouldFailOnUnequalLengths() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> makeExtractions(invalidGraph))
            .withMessageContaining(
                "The property `a` contains arrays of differing lengths `1` and `2`."
            );
    }
}
