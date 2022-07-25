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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
    void contextDoesNotIncludeTargetRelType() {
        var config = LinkPredictionTrainConfigImpl.builder()
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .username("DUMMY")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .contextRelationshipTypes(List.of(ElementProjection.PROJECT_ALL))
            .targetRelationshipType("REL")
            .build();

        assertThat(config.internalContextRelationshipType(graphStore)).containsExactly(RelationshipType.of("CONTEXT"));
    }

    @Test
    void targetRelMustBeUndirected() {
        var config = LinkPredictionTrainConfigImpl.builder()
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .username("DUMMY")
            .sourceNodeLabel(ElementProjection.PROJECT_ALL)
            .targetNodeLabel(ElementProjection.PROJECT_ALL)
            .contextRelationshipTypes(List.of(ElementProjection.PROJECT_ALL))
            .targetRelationshipType("REL")
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            directedGraphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        )).hasMessage("TargetRelationshipType 'REL' must be undirected, but was directed.");
    }

    @Test
    void featureInputNodeLabelsIncludeSourceAndTarget() {
        var config = LinkPredictionTrainConfigImpl.builder()
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .username("DUMMY")
            .sourceNodeLabel("A")
            .targetNodeLabel("B")
            .contextNodeLabels(List.of("C"))
            .contextRelationshipTypes(List.of(ElementProjection.PROJECT_ALL))
            .targetRelationshipType("REL")
            .build();

        var graphStore = GdlFactory.of("(:A)-[:REL]->(:B)-[:CONTEXT]->(:C)").build();


        assertThat(config.featureInputLabels(graphStore)).containsExactly(
            NodeLabel.of("A"),
            NodeLabel.of("B"),
            NodeLabel.of("C")
        );
    }

}
