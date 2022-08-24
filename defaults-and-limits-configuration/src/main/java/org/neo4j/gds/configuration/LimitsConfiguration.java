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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LimitsConfiguration {
    /**
     * A JVM-wide singleton we can use to store the limits configuration. Lifecycle of this singleton matches that of
     * the data it holds - it starts empty and dies when JVM exists.
     */
    public static final LimitsConfiguration Instance = new LimitsConfiguration(
        new HashMap<>(),
        new HashMap<>()
    );

    /**
     * This is a handy singleton for when you need to satisfy a requirement for the configuration, but want to ignore
     * limits entirely.
     */
    public static final LimitsConfiguration Empty = new LimitsConfiguration(
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    private final Map<String, Limit> globalLimits;

    private final Map<String, Map<String, Limit>> personalLimits;

    public LimitsConfiguration(Map<String, Limit> globalLimits, Map<String, Map<String, Limit>> personalLimits) {
        this.globalLimits = globalLimits;
        this.personalLimits = personalLimits;
    }

    public Set<LimitViolation> validate(Map<String, Object> configuration, String username) {
        var limitViolations = new HashSet<LimitViolation>();

        for (Map.Entry<String, Object> inputParameter : configuration.entrySet()) {
            var key = inputParameter.getKey();
            var value = inputParameter.getValue();

            // personal limits take precedence
            if (personalLimits.getOrDefault(username, Collections.emptyMap()).containsKey(key)) {
                Limit limit = personalLimits.get(username).get(key);

                if (limit.isViolated(value)) {
                    limitViolations.add(new LimitViolation(limit.asErrorMessage(key, value)));

                    continue; // we found a violation; skip to next parameter
                }
            }

            // global limits come second
            if (!globalLimits.containsKey(key)) continue;

            var limit = globalLimits.get(key);

            if (limit.isViolated(value)) {
                var limitViolation = new LimitViolation(limit.asErrorMessage(key, value));

                limitViolations.add(limitViolation);
            }
        }

        return limitViolations;
    }

    public Map<String, Object> list(Optional<String> username, Optional<String> key) {
        var limits = startWithGlobalLimits();

        overlayPersonalLimitsIfApplicable(username, limits);

        if (key.isEmpty()) return limits;

        if (!limits.containsKey(key.get())) return Collections.emptyMap(); // done because value does not exist for key

        Object value = limits.get(key.get());

        return Map.of(key.get(), value);
    }

    public void set(String key, Object value, Optional<String> username) {
        var valueAsLimit = LimitFactory.create(value);

        if (username.isPresent()) {
            personalLimits.putIfAbsent(username.get(), new HashMap<>()); // lazy initialisation
            personalLimits.get(username.get()).put(key, valueAsLimit);
        } else {
            globalLimits.put(key, valueAsLimit);
        }
    }

    private Map<String, Object> startWithGlobalLimits() {
        return globalLimits.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }

    private void overlayPersonalLimitsIfApplicable(Optional<String> username, Map<String, Object> defaults) {
        username.ifPresent(s -> personalLimits.getOrDefault(s, Collections.emptyMap())
            .forEach((k, v) -> defaults.put(k, v.getValue())));
    }
}
