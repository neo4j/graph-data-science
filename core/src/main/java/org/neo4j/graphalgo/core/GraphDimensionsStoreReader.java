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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.ANY_RELATIONSHIP_TYPE;

public class GraphDimensionsStoreReader extends GraphDimensionsReader<GraphCreateFromStoreConfig> {

    public GraphDimensionsStoreReader(GraphDatabaseAPI api, GraphCreateFromStoreConfig config) {
        super(api, config);
    }

    @Override
    protected TokenElementIdentifierMappings<NodeLabel> labelTokeNodeLabelMappings(TokenRead tokenRead) {
        final TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings = new TokenElementIdentifierMappings<>(ANY_LABEL);
        graphCreateConfig.nodeProjections()
            .projections()
            .forEach((nodeLabel, projection) -> {
                var labelToken = projection.projectAll() ? ANY_LABEL : getNodeLabelToken(tokenRead, projection);
                labelTokenNodeLabelMappings.put(labelToken, nodeLabel);
            });
        return labelTokenNodeLabelMappings;
    }

    @Override
    protected TokenElementIdentifierMappings<RelationshipType> labelTokenRelationshipTypeMappings(TokenRead tokenRead) {
        final TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings = new TokenElementIdentifierMappings<>(ANY_RELATIONSHIP_TYPE);
        graphCreateConfig.relationshipProjections()
            .projections()
            .forEach((relType, projection) -> {
                var typeToken = projection.projectAll() ? ANY_RELATIONSHIP_TYPE : getRelationshipTypeToken(
                    tokenRead,
                    projection
                );
                typeTokenRelTypeMappings.put(typeToken, relType);
            });
        return typeTokenRelTypeMappings;
    }
}
