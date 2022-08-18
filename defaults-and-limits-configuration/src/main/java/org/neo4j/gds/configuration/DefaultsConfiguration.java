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
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultsConfiguration {
    /**
     * A JVM-wide singleton we can use to store the defaults configuration. Lifecycle of this singleton matches that of
     * the data it holds - it starts empty and dies when JVM exists.
     */
    public static final DefaultsConfiguration Instance = new DefaultsConfiguration(
        new HashMap<>(),
        new HashMap<>()
    );

    /**
     * This is a handy singleton for when you need to satisfy a requirement for the configuration, but want to ignore
     * defaults entirely.
     */
    public static final DefaultsConfiguration Empty = new DefaultsConfiguration(
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    private final Map<String, Default> globalDefaults;

    private final Map<String, Map<String, Default>> personalDefaults;

    public DefaultsConfiguration(
        Map<String, Default> globalDefaults,
        Map<String, Map<String, Default>> personalDefaults
    ) {
        this.globalDefaults = globalDefaults;
        this.personalDefaults = personalDefaults;
    }

    /**
     * Take user input and apply defaults to fill out fields not supplied by user
     */
    public Map<String, Object> apply(Map<String, Object> configuration, String username) {
        Map<String, Object> configurationWithDefaults = new HashMap<>(configuration);

        startWithPersonalDefaults(username, configurationWithDefaults);
        fillInWithGlobalDefaults(configurationWithDefaults);

        return configurationWithDefaults;
    }

    /**
     * List global default settings.
     *
     * @param username if supplied, defaults for this user are overlaid
     * @param key      if supplied, return just the default setting for that key
     */
    public Map<String, Object> list(Optional<String> username, Optional<String> key) {
        var defaults = startWithGlobalDefaults();
        overlayPersonalDefaultsIfApplicable(username, defaults);

        if (key.isEmpty()) return defaults; // if no key specified, we are done

        if (!defaults.containsKey(key.get())) return Collections.emptyMap(); // done because value does not exist for key

        Object value = defaults.get(key.get());

        return Map.of(key.get(), value);
    }

    /**
     * Set default for key to value globally.
     *
     * @param username if supplied, set the key-value pair just for that user.
     */
    void set(String key, Object value, Optional<String> username) {
        Default valueAsDefault = new Default((Long) value);

        if (username.isPresent()) {
            personalDefaults.putIfAbsent(username.get(), new HashMap<>()); // lazy initialisation
            personalDefaults.get(username.get()).put(key, valueAsDefault);
        } else {
            globalDefaults.put(key, valueAsDefault);
        }
    }

    private void fillInWithGlobalDefaults(Map<String, Object> configurationWithDefaults) {
        globalDefaults
            .forEach((s, d) -> configurationWithDefaults.putIfAbsent(s, d.getValue()));
    }

    private void startWithPersonalDefaults(String username, Map<String, Object> configurationWithDefaults) {
        personalDefaults.getOrDefault(username, Collections.emptyMap())
            .forEach((s, d) -> configurationWithDefaults.putIfAbsent(s, d.getValue()));
    }

    private Map<String, Object> startWithGlobalDefaults() {
        return globalDefaults.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }

    private void overlayPersonalDefaultsIfApplicable(Optional<String> username, Map<String, Object> defaults) {
        username.ifPresent(s -> personalDefaults.getOrDefault(s, Collections.emptyMap())
            .forEach((k, v) -> defaults.put(k, v.getValue())));
    }
}
