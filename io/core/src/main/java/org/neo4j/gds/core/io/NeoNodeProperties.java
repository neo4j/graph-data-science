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
package org.neo4j.gds.core.io;

import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

@ValueClass
public interface NeoNodeProperties {

    Map<String, LongFunction<Object>> neoNodeProperties();

    static Optional<NeoNodeProperties> of(
        GraphStore graphStore,
        TransactionContext transactionContext,
        PropertyMappings propertyMappings,
        Log log
    ) {
        if (propertyMappings.isEmpty()) {
            return Optional.empty();
        }

        var neoNodeProperties = propertyMappings
            .stream()
            .collect(Collectors.toMap(
                    PropertyMapping::neoPropertyKey,
                    propertyMapping -> NeoProperties.of(
                        transactionContext,
                        graphStore.nodes(),
                        propertyMapping,
                        log
                    )
                )
            );

        return Optional.of(ImmutableNeoNodeProperties.builder()
            .neoNodeProperties(neoNodeProperties)
            .build());
    }

    final class NeoProperties implements LongFunction<Object> {

        private final TransactionContext transactionContext;
        private final IdMap idMap;
        private final PropertyMapping propertyMapping;
        private final Log log;

        static LongFunction<Object> of(
            TransactionContext transactionContext,
            IdMap idMap,
            PropertyMapping propertyMapping,
            Log log
        ) {
            return new NeoProperties(transactionContext, idMap, propertyMapping, log);
        }

        private NeoProperties(
            TransactionContext transactionContext,
            IdMap idMap,
            PropertyMapping propertyMapping,
            Log log
        ) {
            this.transactionContext = transactionContext;
            this.idMap = idMap;
            this.propertyMapping = propertyMapping;
            this.log = log;
        }

        @Override
        public Object apply(long nodeId) {
            return transactionContext.apply((tx, ktx) -> {
                var neo4jNodeId = idMap.toOriginalNodeId(nodeId);
                try {
                    var node = tx.getNodeById(neo4jNodeId);
                    return node.getProperty(
                        propertyMapping.neoPropertyKey(),
                        propertyMapping.defaultValue().getObject()
                    );
                } catch (NotFoundException e) {
                    var defaultValue = propertyMapping.defaultValue().getObject();

                    // WARN because we have a default value and can proceed.
                    // We don't log the exception to not flood the log with stacktraces.
                    // The exception as it doesn't tell anything more that we already do in the log message.
                    // It is also likely that once we run into missing nodes, we will get more than just one.
                    // Putting a million log lines for a million missing nodes isn't great, but it's better
                    // than putting a million stacktraces into the log.
                    log.warn(
                        StringFormatting.formatWithLocale(
                            "Could not find the node with the id '%d' - using the default value for the property '%s' (%s).",
                            neo4jNodeId,
                            propertyMapping.neoPropertyKey(),
                            defaultValue
                        )
                    );

                    return defaultValue;
                }
            });
        }
    }
}
