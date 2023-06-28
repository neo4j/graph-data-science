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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.core.CypherMapAccess;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * GraphName ought to be a micro type. That would strengthen our domain model. And we could then create a guard,
 * where it could only get constructed in a known good state - i.e. not blank.
 */
public class GraphNameValidationService {
    /**
     * Validates that the input graph name is
     * - not null
     * - not blank
     *
     * @return trimmed graph name for downstream consumption.
     * @throws IllegalArgumentException if graph name is null or blank
     */
    public String validate(String graphName) {
        return CypherMapAccess.failOnBlank("graphName", graphName).trim();
    }

    /**
     * Validates that the input graph name is not blank. Blank is the worst!
     *
     * @return {@link java.util.Optional#empty()} if graph name was null; trimmed graph name otherwise
     * @throws IllegalArgumentException if graph name is blank
     */
    public Optional<String> validatePossibleNull(String graphName) {
        if (graphName == null) return Optional.empty();

        return Optional.of(validate(graphName));
    }

    /**
     * Validates that the input graph name or list of graph names are
     * - not null
     * - not blank
     * Furthermore, the graph name(s) are trimmed and put into a List for downstream consumption.
     */
    public List<String> validateSingleOrList(Object graphNameOrListOfGraphNames) {
        if (graphNameOrListOfGraphNames == null) {
            validate(null);

            throw new IllegalStateException("Yeah that thing above should have thrown an exception");
        }

        if (graphNameOrListOfGraphNames instanceof String) {
            return List.of(validate((String) graphNameOrListOfGraphNames));
        }

        if (graphNameOrListOfGraphNames instanceof Collection<?>) {
            var listOfGraphNames = (Collection<?>) graphNameOrListOfGraphNames;

            var validatedGraphNames = new LinkedList<String>();
            int index = 0;
            for (Object graphName : listOfGraphNames) {
                var validatedGraphName = validateSingleFromList(graphName, index);
                validatedGraphNames.add(validatedGraphName);
                index++;
            }
            return validatedGraphNames;
        }

        throw typeMismatch(graphNameOrListOfGraphNames, -1);
    }

    private String validateSingleFromList(Object graphName, int index) {
        if (graphName == null) {
            validate(null);

            throw new IllegalStateException("Yeah that thing above should have thrown an exception");
        }

        if (graphName instanceof String) return validate((String) graphName);

        throw typeMismatch(graphName, index);
    }

    private IllegalArgumentException typeMismatch(Object invalid, int index) {
        String errorMessage = formatWithLocale(
            "Type mismatch%s: expected String but was %s.",
            index >= 0 ? (" at index " + index) : "",
            invalid == null ? "null" : invalid.getClass().getSimpleName()
        );

        return new IllegalArgumentException(errorMessage);
    }
}
