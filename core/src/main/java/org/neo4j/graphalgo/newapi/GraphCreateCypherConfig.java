/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;

@ValueClass
@Configuration("GraphCreateCypherConfigImpl")
public interface GraphCreateCypherConfig extends GraphCreateBaseConfig {

    @NotNull String IMPLICIT_GRAPH_NAME = "";
    @NotNull String NODE_QUERY_KEY = "nodeQuery";
    @NotNull String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    @NotNull String BLANK_QUERY = "";
    @NotNull String NODE_PROPERTIES_KEY = "nodeProperties";
    @NotNull String RELATIONSHIP_PROPERTIES_KEY = "relationshipProperties";

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

    @Override
    @Configuration.Parameter
    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String nodeQuery();

    @Override
    @Configuration.Parameter
    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String relationshipQuery();

    static GraphCreateCypherConfig of(
        String userName,
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        CypherMapWrapper config
    ) {
        return new GraphCreateCypherConfigImpl(
            nodeQuery,
            relationshipQuery,
            graphName,
            userName,
            config
        );
    }

    static GraphCreateCypherConfig implicitCreate(
        String username,
        CypherMapWrapper config
    ) {
        String nodeQuery = CypherMapWrapper.failOnBlank(NODE_QUERY_KEY, config.getString(NODE_QUERY_KEY, BLANK_QUERY));
        String relationshipQuery = CypherMapWrapper.failOnBlank(RELATIONSHIP_QUERY_KEY, config.getString(RELATIONSHIP_QUERY_KEY, BLANK_QUERY));

        PropertyMappings nodeProperties = (config.containsKey(NODE_PROPERTIES_KEY))
            ? PropertyMappings.fromObject(config.get(NODE_PROPERTIES_KEY, null))
            : PropertyMappings.of();

        PropertyMappings relationshipProperties = (config.containsKey(RELATIONSHIP_PROPERTIES_KEY))
            ? PropertyMappings.fromObject(config.get(RELATIONSHIP_PROPERTIES_KEY, null))
            : PropertyMappings.of();

        return new GraphCreateCypherConfigImpl(
            nodeQuery,
            relationshipQuery,
            IMPLICIT_GRAPH_NAME,
            nodeProperties,
            relationshipProperties,
            Pools.DEFAULT_CONCURRENCY,
            username
        );
    }
}
