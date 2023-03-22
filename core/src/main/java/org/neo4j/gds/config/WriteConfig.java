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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;

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
        String username();
        String password();

        static ArrowConnectionInfo parse(Object input) {
            if (input instanceof Map) {
                var map = (Map<String, String>) input;
                var hostname = map.get("hostname");
                var port = Integer.parseInt(map.get("port"));
                var username = map.get("username");
                var password = map.get("parssword");

                return ImmutableArrowConnectionInfo.of(hostname, port, username, password);
            }
            throw new IllegalArgumentException(formatWithLocale(
                "Expected input to be of type `map`, but got `%s`",
                input.getClass().getSimpleName()
            ));
        }
    }
}
