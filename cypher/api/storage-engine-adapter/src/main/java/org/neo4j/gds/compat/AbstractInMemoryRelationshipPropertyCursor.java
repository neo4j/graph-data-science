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

import org.eclipse.collections.api.list.primitive.DoubleList;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public abstract class AbstractInMemoryRelationshipPropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;
    protected InMemoryPropertySelection selection;
    private DoubleList propertyValues;
    private int[] propertyIds;
    private int index;

    protected AbstractInMemoryRelationshipPropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        reset();
    }

    public void initRelationshipPropertyCursor(long id, int[] propertyIds, DoubleList propertyValues, InMemoryPropertySelection selection) {
        setId(id);
        setRelId(id);
        this.index = -1;
        this.propertyIds = propertyIds;
        this.propertyValues = propertyValues;
        this.selection = selection;
    }

    @Override
    public boolean next() {
        if (getRelId() == NO_ID) {
            throw new IllegalStateException("Property cursor is not initialized.");
        }

        while(true) {
            index++;
            if (index >= propertyIds.length) {
                return false;
            } else if (selection.test(propertyIds[index])) {
                return true;
            }
        }
    }

    @Override
    public int propertyKey() {
        if (getRelId() != NO_ID && this.index < propertyIds.length) {
            return propertyIds[index];
        }
        throw new IllegalStateException("Property cursor is not initialized.");
    }

    @Override
    public ValueGroup propertyType() {
        return ValueGroup.NUMBER;
    }

    @Override
    public Value propertyValue() {
        if (getRelId() != NO_ID && this.index < propertyIds.length) {
            return Values.doubleValue(propertyValues.get(this.index));
        }
        throw new IllegalStateException("Property cursor is not initialized.");
    }

    @Override
    public void reset() {
        this.index = -1;
        this.propertyIds = null;
        this.propertyValues = null;
        setId(NO_ID);
        setRelId(NO_ID);
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }
}
