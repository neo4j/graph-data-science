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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class LinkPredictionTrainConfigTest {

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "directed")
    static final String GDL_GRAPH = "()-[:REL]->()";

    @Inject
    GraphStore directedGraphStore;

    @Test
    void targetRelMustBeUndirected() {
        var config = LinkPredictionTrainConfig.of(
            "user",
            Optional.of("graph"),
            Optional.empty(),
            CypherMapWrapper.create(
                Map.of(
                    "pipeline", "DUMMY",
                    "modelName", "DUMMY"
                )
            ));

        assertThatThrownBy(() -> config.graphStoreValidation(
            directedGraphStore,
            List.of(),
            config.internalRelationshipTypes(directedGraphStore)
        )).hasMessage("RelationshipTypes must be undirected, but found ['REL'] which are directed.");
    }

}

