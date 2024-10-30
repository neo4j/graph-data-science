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
package org.neo4j.gds.legacycypherprojection;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface GraphProjectFromCypherConfig extends GraphProjectConfig {

    @Configuration.Ignore
    default Map<String, Object> asProcedureResultConfigurationField() {
        return cleansed(toMap(), outputFieldDenylist());
    }

    List<String> FORBIDDEN_KEYS = Arrays.asList(
//        GraphProjectFromStoreConfig.NODE_PROJECTION_KEY,
        "nodeProjection",
//        GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY,
        "relationshipProjection",
//        GraphProjectFromStoreConfig.NODE_PROPERTIES_KEY,
        "nodeProperties",
//        GraphProjectFromStoreConfig.RELATIONSHIP_PROPERTIES_KEY
        "relationshipProperties"
    );

    String NODE_QUERY_KEY = "nodeQuery";
    String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    String ALL_NODES_QUERY = "MATCH (n) RETURN id(n) AS id";
    String ALL_RELATIONSHIPS_QUERY = "MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target";

    @Configuration.ConvertWith(method = "org.apache.commons.lang3.StringUtils#trimToNull")
    String nodeQuery();

    @Configuration.ConvertWith(method = "org.apache.commons.lang3.StringUtils#trimToNull")
    String relationshipQuery();

    @Configuration.ToMapValue("org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfig#listParameterKeys")
    default Map<String, Object> parameters() {
        return Collections.emptyMap();
    }

    @Override
    default boolean validateRelationships() {
        return true;
    }

//    @Configuration.Ignore
//    @Override
//    default GraphStoreFactory.Supplier graphStoreFactory() {
//        return new GraphStoreFactory.Supplier() {
//            @Override
//            public GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> get(GraphLoaderContext loaderContext) {
//                return CypherFactory.createWithDerivedDimensions(GraphProjectFromCypherConfig.this, loaderContext);
//            }
//
//            @Override
//            public GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> getWithDimension(
//                GraphLoaderContext loaderContext, GraphDimensions graphDimensions
//            ) {
//                return CypherFactory.createWithBaseDimensions(GraphProjectFromCypherConfig.this, loaderContext, graphDimensions);
//            }
//        };
//    }

    @Override
    default boolean sudo() {
        return true;
    }

    @Configuration.Ignore
    default Set<String> outputFieldDenylist() {
        return Set.of(NODE_COUNT_KEY, RELATIONSHIP_COUNT_KEY);
    }

    static GraphProjectFromCypherConfig of(
        String userName,
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        CypherMapWrapper config
    ) {
        assertNoProjectionsOrExplicitProperties(config);

        if (nodeQuery != null) {
            config = config.withString(NODE_QUERY_KEY, nodeQuery);
        }
        if (relationshipQuery != null) {
            config = config.withString(RELATIONSHIP_QUERY_KEY, relationshipQuery);
        }
        return new GraphProjectFromCypherConfigImpl(
            userName,
            graphName,
            config
        );
    }

    static GraphProjectFromCypherConfig fromProcedureConfig(String username, CypherMapWrapper config) {
        assertNoProjectionsOrExplicitProperties(config);
        return new GraphProjectFromCypherConfigImpl(
            username,
            IMPLICIT_GRAPH_NAME,
            config
        );
    }

    static void assertNoProjectionsOrExplicitProperties(CypherMapWrapper config) {
        for (String forbiddenKey : FORBIDDEN_KEYS) {
            if (config.containsKey(forbiddenKey)) {
                throw new IllegalArgumentException(formatWithLocale("Invalid key: %s", forbiddenKey));
            }
        }
    }

    static Collection<String> listParameterKeys(Map<String, Object> parameters) {
        return parameters.keySet();
    }

}
