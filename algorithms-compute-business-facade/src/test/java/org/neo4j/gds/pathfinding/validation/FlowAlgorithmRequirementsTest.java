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
package org.neo4j.gds.pathfinding.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.validation.OptionalNodePropertyGraphStoreRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.TargetNodesRequirement;

import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FlowAlgorithmRequirementsTest {

    @Test
    void shouldCallCorrectRequirements(){
        var sources = mock(SourceNodesRequirement.class);
        var targets = mock(TargetNodesRequirement.class);
        var nodeCapacity = mock(OptionalNodePropertyGraphStoreRequirement.class);

        var graphStore =  mock(GraphStore.class);
        Collection<NodeLabel> nodeLabels = List.of();
        Collection<RelationshipType> relTypes = List.of();

        new FlowAlgorithmRequirements(sources,targets,nodeCapacity).validate(
            graphStore,
            nodeLabels,
            relTypes
        );

        verify(sources,times(1)).validate(eq(graphStore),eq(nodeLabels),eq(relTypes));
        verify(targets,times(1)).validate(eq(graphStore),eq(nodeLabels),eq(relTypes));
        verify(nodeCapacity,times(1)).validate(eq(graphStore),eq(nodeLabels),eq(relTypes));

    }

}
