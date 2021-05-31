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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.StatementConstants;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.NO_SUCH_RELATIONSHIP_TYPE;

public class GraphDimensionsStoreReader extends GraphDimensionsReader<GraphCreateFromStoreConfig> {

    public GraphDimensionsStoreReader(
        TransactionContext tx,
        GraphCreateFromStoreConfig config,
        IdGeneratorFactory idGeneratorFactory
    ) {
        super(tx, config, idGeneratorFactory);
    }

    @Override
    protected TokenElementIdentifierMappings<NodeLabel> getNodeLabelTokens(TokenRead tokenRead) {
        var labelTokenNodeLabelMappings = new TokenElementIdentifierMappings<NodeLabel>(
            ANY_LABEL);
        graphCreateConfig.nodeProjections()
            .projections()
            .forEach((nodeLabel, projection) -> {
                var labelToken = projection.projectAll() ? ANY_LABEL : getNodeLabelToken(tokenRead, projection.label());
                labelTokenNodeLabelMappings.put(labelToken, nodeLabel);
            });
        return labelTokenNodeLabelMappings;
    }

    @Override
    protected TokenElementIdentifierMappings<RelationshipType> getRelationshipTypeTokens(TokenRead tokenRead) {
        var typeTokenRelTypeMappings = new TokenElementIdentifierMappings<RelationshipType>(
            ANY_RELATIONSHIP_TYPE);
        graphCreateConfig.relationshipProjections()
            .projections()
            .forEach((relType, projection) -> {
                var typeToken = projection.projectAll() ? ANY_RELATIONSHIP_TYPE : getRelationshipTypeToken(
                    tokenRead,
                    projection.type()
                );
                typeTokenRelTypeMappings.put(typeToken, relType);
            });
        return typeTokenRelTypeMappings;
    }

    @Override
    protected NodeProjections getNodeProjections() {
        return graphCreateConfig.nodeProjections();
    }

    @Override
    protected RelationshipProjections getRelationshipProjections() {
        return graphCreateConfig.relationshipProjections();
    }

    private int getNodeLabelToken(TokenRead tokenRead, String nodeLabel) {
        int labelToken = tokenRead.nodeLabel(nodeLabel);
        return labelToken == StatementConstants.NO_SUCH_LABEL
            ? NO_SUCH_LABEL
            : labelToken;
    }

    private int getRelationshipTypeToken(TokenRead tokenRead, String relationshipType) {
        int relationshipToken = tokenRead.relationshipType(relationshipType);
        return relationshipToken == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
            ? NO_SUCH_RELATIONSHIP_TYPE
            : relationshipToken;
    }
}
