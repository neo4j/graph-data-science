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

import org.neo4j.gds.applications.algorithms.machinery.RequestConfiguration;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;

import java.util.Set;

class GraphLoadETLParser {
    private final Set<GraphStoreFilteredETLParser> graphStoreFilteredETLParsers;

    GraphLoadETLParser(Set<GraphStoreFilteredETLParser> graphStoreFilteredETLParsers) {
        this.graphStoreFilteredETLParsers = graphStoreFilteredETLParsers;
    }

    /**
     * @return the parser configured with the full complement of production business rules
     */
    static GraphLoadETLParser create() {
        return new GraphLoadETLParser(
            Set.of(
                new MutateNodePropertyConfigGraphStoreFilteredETLParser(),
                new MutateRelationshipConfigGraphStoreFilteredETLParser()
            )
        );
    }

    void parse(
        RequestConfiguration requestConfiguration,
        GraphLoadETLBuilder graphLoadETLBuilder,
        boolean disableRelationshipTypeTest
    ) {
        for (var parser : graphStoreFilteredETLParsers) {
            var etlStep = parser.parse(requestConfiguration, disableRelationshipTypeTest);

            etlStep.ifPresent(graphLoadETLBuilder::withGraphStoreFilteredETL);
        }
    }
}
