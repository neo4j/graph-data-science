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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractInMemoryPropertyCursor
    extends PropertyRecord implements StoragePropertyCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;
    @SuppressFBWarnings(
        value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
        justification = "Field will be initialized in the compat specific instances during initNodeProperties"
    )
    protected DelegatePropertyCursor<?, ?> delegate;

    public AbstractInMemoryPropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public int propertyKey() {
        return delegate.propertyKey();
    }

    @Override
    public ValueGroup propertyType() {
        return delegate.propertyType();
    }

    @Override
    public Value propertyValue() {
        return delegate.propertyValue();
    }

    @Override
    public boolean next() {
        return delegate.next();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public void setForceLoad() {
        delegate.setForceLoad();
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    abstract static class DelegatePropertyCursor<IDENTIFIER extends ElementIdentifier, PROP_SCHEMA extends PropertySchema>
        extends PropertyRecord implements StoragePropertyCursor {

        protected final CypherGraphStore graphStore;
        protected final TokenHolders tokenHolders;

        final Map<String, ValueGroup> propertyKeyToValueGroupMapping;

        private final Iterator<NamedToken> namedTokensIterator;

        @Nullable String currentPropertyKey;

        private Predicate<Integer> propertySelection;

        DelegatePropertyCursor(long id, CypherGraphStore graphStore, TokenHolders tokenHolders) {
            super(id);
            this.graphStore = graphStore;
            this.tokenHolders = tokenHolders;
            this.propertyKeyToValueGroupMapping = new HashMap<>();
            this.namedTokensIterator = tokenHolders.propertyKeyTokens().getAllTokens().iterator();

            populateKeyToValueGroupMapping();
        }

        protected abstract Map<IDENTIFIER, Map<String, PROP_SCHEMA>> propertySchema();

        protected void setPropertySelection(Predicate<Integer> propertySelection) {
            this.propertySelection = propertySelection;
        }

        @Override
        public int propertyKey() {
            return tokenHolders.propertyKeyTokens().getIdByName(currentPropertyKey);
        }

        @Override
        public ValueGroup propertyType() {
            return propertyKeyToValueGroupMapping.get(currentPropertyKey);
        }

        @Override
        public boolean next() {
            if (getId() != NO_ID) {
                // In Neo4j properties are retrieved by following a pointer
                // to the first property of an element and then following
                // the property chain to the next property and so on.
                // We try to mimic this behaviour by iterating through
                // all available properties in the graph store.
                while (namedTokensIterator.hasNext()) {
                    var namedToken = namedTokensIterator.next();
                    if (propertySelection.test(namedToken.id())) {
                        this.currentPropertyKey = namedToken.name();
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public void reset() {
            clear();
            this.setId(NO_ID);
            this.currentPropertyKey = null;
        }

        @Override
        public void setForceLoad() {

        }

        @Override
        public void close() {

        }

        private void populateKeyToValueGroupMapping() {
            propertySchema()
                .forEach((identifier, propertyMap) ->
                    propertyMap.forEach((propertyKey, propertySchema) ->
                        this.propertyKeyToValueGroupMapping.put(
                            propertyKey,
                            valueGroupFromValueType(propertySchema.valueType())
                        )
                    )
                );
        }

        private static ValueGroup valueGroupFromValueType(ValueType valueType) {
            switch (valueType) {
                case DOUBLE:
                case LONG:
                    return ValueGroup.NUMBER;
                case LONG_ARRAY:
                case DOUBLE_ARRAY:
                case FLOAT_ARRAY:
                    return ValueGroup.NUMBER_ARRAY;
                default:
                    return ValueGroup.UNKNOWN;
            }
        }
    }

}
