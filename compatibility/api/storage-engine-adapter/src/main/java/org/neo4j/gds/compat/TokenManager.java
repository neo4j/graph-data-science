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

import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;

import java.util.HashSet;

public class TokenManager implements CypherGraphStore.StateVisitor {

    private final TokenHolders tokenHolders;
    private final InMemoryTransactionStateVisitor transactionStateVisitor;
    private final CypherGraphStore graphStore;
    private final CommandCreationContext commandCreationContext;

    public TokenManager(
        TokenHolders tokenHolders,
        InMemoryTransactionStateVisitor transactionStateVisitor,
        CypherGraphStore graphStore,
        CommandCreationContext commandCreationContext
    ) {
        this.tokenHolders = tokenHolders;
        this.transactionStateVisitor = transactionStateVisitor;
        this.graphStore = graphStore;
        this.commandCreationContext = commandCreationContext;

        init();
    }

    public void init() {
        initializeTokensFromGraphStore();
        graphStore.registerStateVisitor(this);
    }

    @Override
    public void nodePropertyRemoved(String propertyKey) {
        this.transactionStateVisitor.removeNodeProperty(propertyKey);
    }

    @Override
    public void nodePropertyAdded(String propertyKey) {
        getOrCreatePropertyToken(propertyKey);
    }

    @Override
    public void nodeLabelAdded(String nodeLabel) {
        try {
            tokenHolders.labelTokens().getOrCreateId(nodeLabel);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void relationshipTypeAdded(String relationshipType) {
        try {
            tokenHolders.relationshipTypeTokens().getOrCreateId(relationshipType);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void relationshipPropertyAdded(String relationshipProperty) {
        getOrCreatePropertyToken(relationshipProperty);
    }

    private void getOrCreatePropertyToken(String propertyKey) {
        try {
            tokenHolders.propertyKeyTokens().getOrCreateId(propertyKey);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    public TokenHolders tokenHolders() {
        return this.tokenHolders;
    }

    private void initializeTokensFromGraphStore() {
        // When this method is called there is no kernel available
        // which is needed to use the `getOrCreate` method on
        // the `TokenHolders`

        var propertyKeys = new HashSet<>(graphStore.nodePropertyKeys());
        propertyKeys.addAll(graphStore.relationshipPropertyKeys());
        propertyKeys.forEach(propertyKey ->
            tokenHolders
                .propertyKeyTokens()
                .addToken(new NamedToken(propertyKey, commandCreationContext.reservePropertyKeyTokenId()))
        );

        graphStore
            .nodeLabels()
            .forEach(nodeLabel -> tokenHolders
                .labelTokens()
                .addToken(new NamedToken(nodeLabel.name(), commandCreationContext.reserveLabelTokenId())));

        graphStore
            .relationshipTypes()
            .forEach(relType -> tokenHolders
                .relationshipTypeTokens()
                .addToken(new NamedToken(relType.name(), commandCreationContext.reserveRelationshipTypeTokenId())));
    }


}
