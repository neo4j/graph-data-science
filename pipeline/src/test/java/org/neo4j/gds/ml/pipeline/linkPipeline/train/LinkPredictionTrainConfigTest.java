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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class LinkPredictionTrainConfigTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "directed")
    static final String GDL_GRAPH = "()-[:REL]->()-[:CONTEXT]->()";

    @Inject
    GraphStore graphStore;

    @Inject
    GraphStore directedGraphStore;

    @Test
    void targetRelMustBeUndirected() {
        var config = LinkPredictionTrainConfigImpl.builder()
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .modelUser("DUMMY")
            .sourceNodeLabel(ElementProjection.PROJECT_ALL)
            .targetNodeLabel(ElementProjection.PROJECT_ALL)
            .targetRelationshipType("REL")
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            directedGraphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        )).hasMessage("Target relationship type `REL` must be UNDIRECTED, but was directed.");
    }

}
