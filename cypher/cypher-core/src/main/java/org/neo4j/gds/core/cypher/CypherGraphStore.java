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
package org.neo4j.gds.core.cypher;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CypherGraphStore extends GraphStoreAdapter implements NodeLabelUpdater {

    private final CypherIdMap cypherIdMap;
    private final Map<NodeLabel, Map<String, UpdatableNodeProperty>> updatableNodeProperties;

    private RelationshipIds relationshipIds;

    public CypherGraphStore(GraphStore graphStore) {
        super(graphStore);
        this.cypherIdMap = new CypherIdMap(super.nodes());
        this.updatableNodeProperties = new HashMap<>();
    }

    public void initialize(TokenHolders tokenHolders) {
        this.relationshipIds = RelationshipIds.fromGraphStore(innerGraphStore(), tokenHolders);
    }

    @Override
    public IdMap nodes() {
        return this.cypherIdMap;
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        this.cypherIdMap.addNodeLabel(nodeLabel);
    }

    @Override
    public void addLabelToNode(long nodeId, NodeLabel nodeLabel) {
        this.cypherIdMap.addLabelToNode(nodeId, nodeLabel);
    }

    public void addNodePropertyToNode(long nodeId, String propertyKey, Value value) {
        nodes().forEachNodeLabel(nodeId, nodeLabel -> {
            var nodeProperty = nodeProperty(nodeLabel, propertyKey);
            if (!(nodeProperty instanceof UpdatableNodeProperty)) {
                throw new UnsupportedOperationException(formatWithLocale("Cannot update immutable property %s", propertyKey));
            }
            ((UpdatableNodeProperty) nodeProperty).updatePropertyValue(nodeId, value);
            return true;
        });
    }

    public Map<NodeLabel, Map<String, UpdatableNodeProperty>> updatableNodeProperties() {
        return this.updatableNodeProperties;
    }

    public RelationshipIds relationshipIds() {
        return this.relationshipIds;
    }
}
