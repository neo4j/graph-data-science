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

import java.util.function.Consumer;

/**
 * This helps with type safety and expressivity for collections of rules.
 */
public final class ValidationRules {
    public static final ValidationRules None = new ValidationRules(new ValidationRule[0]);

    private final ValidationRule[] validationRules;

    private ValidationRules(ValidationRule[] validationRules) {this.validationRules = validationRules;}

    public static ValidationRules from(ValidationRule... validationRules) {
        return new ValidationRules(validationRules);
    }

    public void forEach(Consumer<ValidationRule> action) {
        for (var validationRule : validationRules) {
            action.accept(validationRule);
        }
    }
}
