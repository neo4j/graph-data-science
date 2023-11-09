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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
public
interface ArrowConnectionInfo {
    String hostname();

    int port();

    String bearerToken();

    @Value.Default
    default boolean useEncryption() {
        return true;
    }

    static @Nullable ArrowConnectionInfo parse(Object input) {
        if (input instanceof Map) {
            var map = CypherMapWrapper.create((Map<String, Object>) input);
            var hostname = map.getString("hostname").orElseThrow();
            var port = map.getLongAsInt("port");
            var bearerToken = map.getString("bearerToken").orElseThrow();
            var useEncryption = map.getBool("useEncryption", true);

            return ImmutableArrowConnectionInfo.of(hostname, port, bearerToken, useEncryption);
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
        throw new IllegalArgumentException(
            formatWithLocale(
                "Expected input to be of type `map`, but got `%s`",
                input.getClass().getSimpleName()
            )
        );
    }

    static Map<String, Object> toMap(ArrowConnectionInfo info) {
        Map<String, Object> map = new HashMap<>();
        map.put("hostname", info.hostname());
        map.put("port", info.port());
        map.put("useEncryption", info.useEncryption());
        return map;
    }
}
