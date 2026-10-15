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
package org.neo4j.gds.applications.algorithms.execution;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.RequestConfiguration;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreFilteredETL;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class GraphLoadETLParserTest {
    @Test
    void shouldParseAndPopulateBuilder() {
        var parser1 = mock(GraphStoreFilteredETLParser.class);
        var parser2 = mock(GraphStoreFilteredETLParser.class);
        var parser3 = mock(GraphStoreFilteredETLParser.class);
        var parsers = new HashSet<GraphStoreFilteredETLParser>();
        parsers.add(parser1);
        parsers.add(parser2);
        parsers.add(parser3);
        var parser = new GraphLoadETLParser(parsers);

        var requestConfiguration = mock(RequestConfiguration.class);
        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        when(parser1.parse(requestConfiguration, false)).thenReturn(Optional.empty());
        var parsedEtlStep = new GraphStoreFilteredETL() {
            @Override
            public void onGraphStoreFiltered(
                GraphStore graphStore,
                Collection<NodeLabel> selectedLabels,
                Collection<RelationshipType> selectedRelationshipTypes,
                Optional<String> relationshipProperty
            ) {
                // do nothing
            }
        };
        when(parser2.parse(requestConfiguration, false)).thenReturn(Optional.of(parsedEtlStep));
        when(parser3.parse(requestConfiguration, false)).thenReturn(Optional.empty());
        parser.parse(requestConfiguration, graphLoadETLBuilder, false);

        verify(graphLoadETLBuilder).withGraphStoreFilteredETL(parsedEtlStep);
    }

    @Test
    void shouldParseUninterestingConfiguration() {
        var parser = GraphLoadETLParser.create();

        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            mock(RequestConfiguration.class),
            graphLoadETLBuilder,
            false
        );

        verifyNoInteractions(graphLoadETLBuilder);
    }

    @Test
    void shouldParseNodePropertyNonExistenceRequirement() {
        var parser = GraphLoadETLParser.create();

        var requestConfiguration = mock(RequestConfiguration.class);
        when(requestConfiguration.mutateNodeProperty()).thenReturn(Optional.of("my property"));
        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            requestConfiguration,
            graphLoadETLBuilder,
            false
        );

        verify(graphLoadETLBuilder).withGraphStoreFilteredETL(any(GraphStoreFilteredETL.class));
    }

    @Test
    void shouldParseRelationshipTypeNonExistenceRequirement() {
        var parser = GraphLoadETLParser.create();

        var requestConfiguration = mock(RequestConfiguration.class);
        when(requestConfiguration.mutateRelationshipType()).thenReturn(Optional.of("my relationship"));
        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            requestConfiguration,
            graphLoadETLBuilder,
            false
        );

        verify(graphLoadETLBuilder).withGraphStoreFilteredETL(any(GraphStoreFilteredETL.class));
    }
}
