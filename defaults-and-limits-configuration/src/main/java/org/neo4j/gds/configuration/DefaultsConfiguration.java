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
package org.neo4j.gds.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultsConfiguration {
    private final Map<String, Default> globalDefaults;
    private final Map<String, Map<String, Default>> personalDefaults;

    public DefaultsConfiguration(
        Map<String, Default> globalDefaults,
        Map<String, Map<String, Default>> personalDefaults
    ) {
        this.globalDefaults = globalDefaults;
        this.personalDefaults = personalDefaults;
    }

    public Map<String, Object> apply(Map<String, Object> configuration, String username) {
        Map<String, Object> configurationWithDefaults = new HashMap<>(configuration);

        personalDefaults.getOrDefault(username, Collections.emptyMap())
            .forEach((s, d) -> configurationWithDefaults.putIfAbsent(s, d.getValue()));
        globalDefaults
            .forEach((s, d) -> configurationWithDefaults.putIfAbsent(s, d.getValue()));

        return configurationWithDefaults;
    }
}
