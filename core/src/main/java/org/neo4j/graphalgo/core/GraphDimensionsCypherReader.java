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
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenRead;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.ANY_RELATIONSHIP_TYPE;

public class GraphDimensionsCypherReader extends GraphDimensionsReader<GraphCreateFromCypherConfig> {

    public GraphDimensionsCypherReader(
        TransactionContext tx,
        GraphCreateFromCypherConfig config,
        IdGeneratorFactory idGeneratorFactory
    ) {
        super(tx, config, idGeneratorFactory);
    }

    @Override
    protected TokenElementIdentifierMappings<NodeLabel> getNodeLabelTokens(TokenRead tokenRead) {
        return new TokenElementIdentifierMappings<>(ANY_LABEL);
    }

    @Override
    protected TokenElementIdentifierMappings<RelationshipType> getRelationshipTypeTokens(TokenRead tokenRead) {
        return new TokenElementIdentifierMappings<>(ANY_RELATIONSHIP_TYPE);
    }

    @Override
    protected NodeProjections getNodeProjections() {
        return NodeProjections.all();
    }

    @Override
    protected RelationshipProjections getRelationshipProjections() {
        return RelationshipProjections.all();
    }
}
