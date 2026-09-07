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

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.validation.GraphStoreValidationBuilder;

import java.util.Set;

class ValidationRuleFromConfigurationParser {
    private final Set<ValidationRuleParser> validationRuleParsers;

    ValidationRuleFromConfigurationParser(Set<ValidationRuleParser> validationRuleParsers) {
        this.validationRuleParsers = validationRuleParsers;
    }

    /**
     * @return the parser configured with the full complement of production business rules
     */
    static ValidationRuleFromConfigurationParser create() {
        return new ValidationRuleFromConfigurationParser(
            Set.of(
                new MutateNodePropertyConfigValidationRuleParser(),
                new MutateRelationshipConfigValidationRuleParser()
            )
        );
    }

    <CONFIGURATION extends AlgoBaseConfig> GraphStoreValidationBuilder parse(
        CONFIGURATION configuration
    ) {
        var builder = new GraphStoreValidationBuilder();

        parse(configuration, builder);

        return builder;
    }

    <CONFIGURATION extends AlgoBaseConfig> void parse(
        CONFIGURATION configuration,
        GraphStoreValidationBuilder builder
    ) {
        for (var parser : validationRuleParsers) {
            var rule = parser.parse(configuration);

            rule.ifPresent(builder::withValidationRule);
        }
    }
}
