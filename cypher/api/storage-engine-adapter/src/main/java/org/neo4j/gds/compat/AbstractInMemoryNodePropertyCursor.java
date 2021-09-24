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
package org.neo4j.gds.compat;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractInMemoryNodePropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor<NodeLabel, PropertySchema> {

    public AbstractInMemoryNodePropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID, graphStore, tokenHolders);
    }

    @Override
    protected Map<NodeLabel, Map<String, PropertySchema>> propertySchema() {
        return graphStore.schema().nodeSchema().properties();
    }

    @Override
    public Value propertyValue() {
        if (currentPropertyKey != null) {
            return graphStore.nodePropertyValues(currentPropertyKey).value(getId());
        } else {
            throw new IllegalStateException(
                "Property cursor is initialized as node and relationship cursor, maybe you forgot to `reset()`?");
        }
    }

    @Override
    protected void setPropertySelection(Predicate<Integer> propertySelection) {
        super.setPropertySelection(
            propertyToken -> {
                try {
                    var namedPropertyToken = tokenHolders.propertyKeyTokens().getTokenById(propertyToken);
                    return propertySelection.test(propertyToken) && propertyPresentOnNode(namedPropertyToken);
                } catch (TokenNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    private boolean propertyPresentOnNode(NamedToken tokenHolder) {
        return graphStore.hasNodeProperty(
            graphStore.nodes().nodeLabels(getId()),
            tokenHolder.name()
        );
    }
}
