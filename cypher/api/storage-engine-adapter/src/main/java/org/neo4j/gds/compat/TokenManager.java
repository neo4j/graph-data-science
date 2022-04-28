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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;

import java.util.HashSet;

class TokenManager extends LifecycleAdapter implements CypherGraphStore.StateVisitor {

    private final TokenHolders tokenHolders;
    private final InMemoryTransactionStateVisitor transactionStateVisitor;
    private final CypherGraphStore graphStore;

    TokenManager(
        TokenHolders tokenHolders,
        InMemoryTransactionStateVisitor transactionStateVisitor,
        CypherGraphStore graphStore
    ) {
        this.tokenHolders = tokenHolders;
        this.transactionStateVisitor = transactionStateVisitor;
        this.graphStore = graphStore;

        init();
    }

    @Override
    public void init() {
        initializeTokensFromGraphStore();
        graphStore.initialize(tokenHolders);
        graphStore.registerStateVisitor(this);
    }

    @Override
    public void nodePropertyRemoved(String propertyKey) {
        this.transactionStateVisitor.removeNodeProperty(propertyKey);
    }

    @Override
    public void nodePropertyAdded(String propertyKey) {
        addProperty(propertyKey);
    }

    @Override
    public void nodeLabelAdded(String nodeLabel) {
        addNodeLabel(nodeLabel);
    }

    @Override
    public void relationshipTypeAdded(String relationshipType) {
        addRelationshipType(relationshipType);
    }

    public TokenHolders tokenHolders() {
        return this.tokenHolders;
    }

    private void addProperty(String propertyName) {
        try {
            tokenHolders.propertyKeyTokens().getOrCreateId(propertyName);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void addNodeLabel(String nodeLabel) {
        try {
            tokenHolders.labelTokens().getOrCreateId(nodeLabel);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void addRelationshipType(String relationshipType) {
        try {
            tokenHolders.relationshipTypeTokens().getOrCreateId(relationshipType);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeTokensFromGraphStore() {
        // When this method is called there is no kernel available
        // which is needed to use the `getOrCreate` method on
        // the `TokenHolders`
        var labelCounter = new MutableInt(0);
        var typeCounter = new MutableInt(0);
        var propertyCounter = new MutableInt(0);

        var propertyKeys = new HashSet<>(graphStore.nodePropertyKeys());
        propertyKeys.addAll(graphStore.relationshipPropertyKeys());
        propertyKeys.forEach(propertyKey ->
            tokenHolders
                .propertyKeyTokens()
                .addToken(new NamedToken(propertyKey, propertyCounter.getAndIncrement()))
        );

        graphStore
            .nodeLabels()
            .forEach(nodeLabel -> tokenHolders
                .labelTokens()
                .addToken(new NamedToken(nodeLabel.name(), labelCounter.getAndIncrement())));

        graphStore
            .relationshipTypes()
            .forEach(relType -> tokenHolders
                .relationshipTypeTokens()
                .addToken(new NamedToken(relType.name(), typeCounter.getAndIncrement())));
    }


}
