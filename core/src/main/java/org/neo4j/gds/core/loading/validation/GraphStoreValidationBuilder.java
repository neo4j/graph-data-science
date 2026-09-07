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
package org.neo4j.gds.core.loading.validation;

import java.util.Collection;
import java.util.HashSet;

public class GraphStoreValidationBuilder {
    private final Collection<ValidationRule> validationRules = new HashSet<>();

    public GraphStoreValidationBuilder withValidationRule(ValidationRule validationRule) {
        validationRules.add(validationRule);
        return this;
    }

    /**
     * @return {@link org.neo4j.gds.core.loading.validation.GraphStoreValidation#DISABLED} if there were no requirements
     */
    public GraphStoreValidation build() {
        if (validationRules.isEmpty()) return GraphStoreValidation.DISABLED;

        return new GraphStoreValidation(new CompoundValidationRule(validationRules));
    }
}
