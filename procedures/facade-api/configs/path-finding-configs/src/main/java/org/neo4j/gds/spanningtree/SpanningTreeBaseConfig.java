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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SourceNodeConfig;

import java.util.Collection;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface SpanningTreeBaseConfig extends
    AlgoBaseConfig,
    RelationshipWeightConfig,
    SourceNodeConfig {

    @Configuration.ConvertWith(method = "org.neo4j.gds.spanningtree.SpanningTreeCompanion#parse")
    @Configuration.ToMapValue("org.neo4j.gds.spanningtree.SpanningTreeCompanion#toString")
    default DoubleUnaryOperator objective() {
        return PrimOperators.MIN_OPERATOR;
    }

    @Configuration.Ignore
    default SpanningTreeParameters toParameters() {
        return new SpanningTreeParameters(objective(), sourceNode());
    }

    @Configuration.GraphStoreValidationCheck
    default void validateUndirectedGraph(
        GraphStore graphStore,
        Collection<NodeLabel> ignored,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var schema = graphStore.schema();
        if(schema.isUndirected()) {
            // nothing to see here, don't go filtering unnecessary
            return;
        }

        if (!schema.filterRelationshipTypes(Set.copyOf(selectedRelationshipTypes)).isUndirected()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "The Spanning Tree algorithm works only with undirected graphs. Selected relationships `%s` are not all undirected. Please orient the edges properly",
                    selectedRelationshipTypes.stream().map(RelationshipType::name).collect(Collectors.toSet())
                ));
        }
    }

}
