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
import org.neo4j.gds.core.loading.validation.ValidationRule;
import org.neo4j.gds.core.loading.validation.GraphStoreValidationBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ValidationRuleFromConfigurationParserTest {
    @Test
    void shouldParseRequirements() {
        var parser1 = mock(ValidationRuleParser.class);
        var parser2 = mock(ValidationRuleParser.class);
        var parser3 = mock(ValidationRuleParser.class);
        var parsers = new HashSet<ValidationRuleParser>();
        parsers.add(parser1);
        parsers.add(parser2);
        parsers.add(parser3);
        var ruleParser = new ValidationRuleFromConfigurationParser(parsers);

        var builder = mock(GraphStoreValidationBuilder.class);
        var configuration = new AlgoBaseConfig() {
            @Override
            public Optional<String> usernameOverride() {
                throw new UnsupportedOperationException("TODO");
            }
        };
        when(parser1.parse(configuration)).thenReturn(Optional.empty());
        var validationRule = new ValidationRule() {
            @Override
            public void validate(
                GraphStore graphStore,
                Collection<NodeLabel> selectedLabels,
                Collection<RelationshipType> selectedRelationshipTypes
            ) {
                // do nothing
            }
        };
        when(parser2.parse(configuration)).thenReturn(Optional.of(validationRule));
        when(parser3.parse(configuration)).thenReturn(Optional.empty());
        ruleParser.parse(configuration, builder);

        verify(builder).withValidationRule(validationRule);
    }

    @Test
    void shouldParseUninterestingConfiguration() {
        var ruleParser = ValidationRuleFromConfigurationParser.create();

        var graphStoreValidation = ruleParser.parse(new AlgoBaseConfig() {
            @Override
            public Optional<String> usernameOverride() {
                throw new UnsupportedOperationException("TODO");
            }
        }).build();

        assertEquals(GraphStoreValidation.DISABLED, graphStoreValidation);
    }

    @Test
    void shouldParseNodePropertyNonExistenceRequirement() {
        var ruleParser = ValidationRuleFromConfigurationParser.create();

        var graphStoreValidation = ruleParser.parse(new MutateNodePropertyConfig() {
            @Override
            public String mutateProperty() {
                return "anything goes";
            }

            @Override
            public Optional<String> usernameOverride() {
                throw new UnsupportedOperationException("TODO");
            }
        }).build();

        assertNotEquals(GraphStoreValidation.DISABLED, graphStoreValidation);
    }

    @Test
    void shouldParseRelationshipTypeNonExistenceRequirement() {
        var ruleParser = ValidationRuleFromConfigurationParser.create();

        var graphStoreValidation = ruleParser.parse(new MutateRelationshipConfig() {
            @Override
            public String mutateRelationshipType() {
                return "go anywhere";
            }

            @Override
            public Optional<String> usernameOverride() {
                throw new UnsupportedOperationException("TODO");
            }
        }).build();

        assertNotEquals(GraphStoreValidation.DISABLED, graphStoreValidation);
    }
}
