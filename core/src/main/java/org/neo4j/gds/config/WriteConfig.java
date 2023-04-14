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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface WriteConfig extends ConcurrencyConfig {

    String WRITE_CONCURRENCY_KEY = "writeConcurrency";

    @Value.Default
    @Configuration.Key(WRITE_CONCURRENCY_KEY)
    default int writeConcurrency() {
        return concurrency();
    }

    @Value.Check
    default void validateWriteConcurrency() {
        ConcurrencyValidatorService
            .validator()
            .validate(writeConcurrency(), WRITE_CONCURRENCY_KEY, ConcurrencyConfig.CONCURRENCY_LIMITATION);
    }

    /**
     * This config option will only exist temporarily for testing the "serverless"
     * architecture. It should only be used for arrow write-back.
     * In the final version we will no longer pass this information in procedure
     * calls, but replace those calls with an arrow protocol.
     * Note that not every write-back path supports forwarding this information to the
     * export builders.
     */
    @Configuration.ConvertWith(method = "org.neo4j.gds.config.WriteConfig.ArrowConnectionInfo#parse")
    Optional<ArrowConnectionInfo> arrowConnectionInfo();

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void validateGraphIsSuitableForWrite(
        GraphStore graphStore,
        @SuppressWarnings("unused") Collection<NodeLabel> selectedLabels,
        @SuppressWarnings("unused") Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (!graphStore.capabilities().canWriteToDatabase()) {
            throw new IllegalArgumentException("The provided graph does not support `write` execution mode.");
        }
    }

    @ValueClass
    interface ArrowConnectionInfo {
        String hostname();
        int port();
        String bearerToken();

        static @Nullable ArrowConnectionInfo parse(Object input) {
            if (input instanceof Map) {
                var map = CypherMapWrapper.create((Map<String, Object>) input);
                var hostname = map.getString("hostname").orElseThrow();
                var port = map.getLongAsInt("port");
                var bearerToken = map.getString("bearerToken").orElseThrow();

                return ImmutableArrowConnectionInfo.of(hostname, port, bearerToken);
            }
            if (input instanceof Optional<?>) {
                Optional<?> optionalInput = (Optional<?>) input;
                if (optionalInput.isEmpty()) {
                    return null;
                } else {
                    var content = optionalInput.get();
                    if (content instanceof ArrowConnectionInfo) {
                        return (ArrowConnectionInfo) content;
                    }
                }
            }
            if (input instanceof ArrowConnectionInfo) {
                return (ArrowConnectionInfo) input;
            }
            throw new IllegalArgumentException(formatWithLocale(
                "Expected input to be of type `map`, but got `%s`",
                input.getClass().getSimpleName()
            ));
        }
    }
}
