/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.newapi;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;

@ValueClass
@Configuration("GraphCreateFromCypherConfigImpl")
public interface GraphCreateFromCypherConfig extends GraphCreateConfig {

    String NODE_QUERY_KEY = "nodeQuery";
    String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    String ALL_NODES_QUERY = "MATCH (n) RETURN id(n) AS id";
    String ALL_RELATIONSHIPS_QUERY = "MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target";

    @Override
    @Configuration.Ignore
    default Class<? extends GraphFactory> getGraphImpl() {
        return CypherGraphFactory.class;
    }

    @Override
    @Configuration.Ignore
    default NodeProjections nodeProjection() {
        return NodeProjections.of();
    }

    @Override
    @Configuration.Ignore
    default RelationshipProjections relationshipProjection() {
        return RelationshipProjections.of();
    }

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String nodeQuery();

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String relationshipQuery();

    static GraphCreateFromCypherConfig of(
        String userName,
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        CypherMapWrapper config
    ) {
        if (nodeQuery != null) {
            config = config.withString(NODE_QUERY_KEY, nodeQuery);
        }
        if (relationshipQuery != null) {
            config = config.withString(RELATIONSHIP_QUERY_KEY, relationshipQuery);
        }
        return new GraphCreateFromCypherConfigImpl(
            graphName,
            userName,
            config
        );
    }

    @TestOnly
    static GraphCreateFromCypherConfig emptyWithName(String userName, String graphName) {
        return GraphCreateFromCypherConfig.of(
            userName,
            graphName,
            ALL_NODES_QUERY,
            ALL_RELATIONSHIPS_QUERY,
            CypherMapWrapper.empty()
        );
    }

    static GraphCreateFromCypherConfig fromProcedureConfig(String username, CypherMapWrapper config) {
        return new GraphCreateFromCypherConfigImpl(
            IMPLICIT_GRAPH_NAME,
            username,
            config
        );
    }
}
