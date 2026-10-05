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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreFilteredETL;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ValidationRuleFromConfigurationParserTest {
    @Test
    void shouldParseAndPopulateBuilder() {
        var parser1 = mock(ValidationRuleParser.class);
        var parser2 = mock(ValidationRuleParser.class);
        var parser3 = mock(ValidationRuleParser.class);
        var parsers = new HashSet<ValidationRuleParser>();
        parsers.add(parser1);
        parsers.add(parser2);
        parsers.add(parser3);
        var parser = new ValidationRuleFromConfigurationParser(parsers);

        var configuration = new AlgoBaseConfig() {
            @Override
            public Optional<String> usernameOverride() {
                throw new UnsupportedOperationException("TODO");
            }
        };
        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        when(parser1.parse(configuration)).thenReturn(Optional.empty());
        var etlStep = new GraphStoreFilteredETL() {
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
        when(parser2.parse(configuration)).thenReturn(Optional.of(etlStep));
        when(parser3.parse(configuration)).thenReturn(Optional.empty());
        parser.parse(configuration, graphLoadETLBuilder);

        verify(graphLoadETLBuilder).with(etlStep);
    }

    @Test
    void shouldParseUninterestingConfiguration() {
        var parser = ValidationRuleFromConfigurationParser.create();

        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            new AlgoBaseConfig() {
                @Override
                public Optional<String> usernameOverride() {
                    throw new UnsupportedOperationException("TODO");
                }
            },
            graphLoadETLBuilder
        );

        verify(graphLoadETLBuilder).with(any(GraphStoreFilteredETL.class));
    }

    @Test
    void shouldParseNodePropertyNonExistenceRequirement() {
        var parser = ValidationRuleFromConfigurationParser.create();

        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            new MutateNodePropertyConfig() {
                @Override
                public String mutateProperty() {
                    return "anything goes";
                }

                @Override
                public Optional<String> usernameOverride() {
                    throw new UnsupportedOperationException("TODO");
                }
            },
            graphLoadETLBuilder
        );

        verify(graphLoadETLBuilder, times(2)).with(any(GraphStoreFilteredETL.class));
    }

    @Test
    void shouldParseRelationshipTypeNonExistenceRequirement() {
        var parser = ValidationRuleFromConfigurationParser.create();

        var graphLoadETLBuilder = mock(GraphLoadETLBuilder.class);
        parser.parse(
            new MutateRelationshipConfig() {
                @Override
                public String mutateRelationshipType() {
                    return "go anywhere";
                }

                @Override
                public Optional<String> usernameOverride() {
                    throw new UnsupportedOperationException("TODO");
                }
            },
            graphLoadETLBuilder
        );

        verify(graphLoadETLBuilder, times(2)).with(any(GraphStoreFilteredETL.class));
    }
}
