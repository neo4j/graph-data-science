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
package org.neo4j.gds.catalog;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.beta.filter.expression.ExpressionParser;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.beta.filter.expression.ValidationContext;
import org.opencypher.v9_0.parser.javacc.ParseException;

final class NodeFilterParser {

    private NodeFilterParser() {}

    static Expression parseAndValidate(GraphStore graphStore, String nodeFilter) throws IllegalArgumentException {
        try {
            var validationContext = ValidationContext.forNodes(graphStore);
            var filter = ExpressionParser.parse(
                nodeFilter.equals(ElementProjection.PROJECT_ALL) ? "true" : nodeFilter,
                validationContext.availableProperties()
            );
            filter.validate(validationContext).validate();
            return filter;
        } catch (ParseException | SemanticErrors e) {
            throw new IllegalArgumentException("Invalid `nodeFilter` expression.", e);
        }
    }

}
