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
package org.neo4j.gds.projection;

import org.intellij.lang.annotations.PrintFormat;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.virtual.MapValue;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ConfigValidator {
    private static final Set<String> DATA_CONFIG_KEYS = Set.of(
        CypherAggregationUpdater.SOURCE_NODE_PROPERTIES,
        CypherAggregationUpdater.SOURCE_NODE_LABELS,
        CypherAggregationUpdater.TARGET_NODE_PROPERTIES,
        CypherAggregationUpdater.TARGET_NODE_LABELS,
        CypherAggregationUpdater.RELATIONSHIP_PROPERTIES,
        CypherAggregationUpdater.RELATIONSHIP_TYPE
    );

    private static final Set<String> PROJECTION_CONFIG_KEYS = Set.copyOf(
        GraphProjectFromCypherAggregationConfig.of("", "", "", MapValue.EMPTY).configKeys()
    );

    private final AtomicBoolean validate = new AtomicBoolean(true);

    void validateConfig(AnyValue dataConfig, AnyValue projectionConfig, AnyValue migrationConfig) {
        if (dataConfig instanceof MapValue || projectionConfig instanceof MapValue) {
            if (this.validate.get()) {
                if (this.validate.getAndSet(false)) {
                    if (dataConfig instanceof MapValue) {
                        validateDataConfig((MapValue) dataConfig, projectionConfig);
                    }
                    if (projectionConfig instanceof MapValue) {
                        validateProjectionConfig((MapValue) projectionConfig, migrationConfig);
                    }
                }
            }
        }
    }

    private void validateDataConfig(MapValue dataConfig, AnyValue projectionConfig) {
        checkForNotMigratedConfigKeys(dataConfig);

        // most map implementation create a new collection, unlike what most Java collections might do, so we cache it.
        var dataConfigKeys = mapKeys(dataConfig);

        checkForMergedOrSwappedOrForgottenConfig(projectionConfig, dataConfigKeys);

        ConfigKeyValidation.requireOnlyKeysFrom(DATA_CONFIG_KEYS, dataConfigKeys);

        checkForMutuallyRequiredKeys(
            dataConfig,
            CypherAggregationUpdater.SOURCE_NODE_LABELS,
            CypherAggregationUpdater.TARGET_NODE_LABELS
        );
        checkForMutuallyRequiredKeys(
            dataConfig,
            CypherAggregationUpdater.TARGET_NODE_LABELS,
            CypherAggregationUpdater.SOURCE_NODE_LABELS
        );
        checkForMutuallyRequiredKeys(
            dataConfig,
            CypherAggregationUpdater.SOURCE_NODE_PROPERTIES,
            CypherAggregationUpdater.TARGET_NODE_PROPERTIES
        );
        checkForMutuallyRequiredKeys(
            dataConfig,
            CypherAggregationUpdater.TARGET_NODE_PROPERTIES,
            CypherAggregationUpdater.SOURCE_NODE_PROPERTIES
        );
    }

    private void validateProjectionConfig(MapValue projectionConfig, AnyValue migrationConfig) {
        var containsRelationshipKeys = projectionConfig.containsKey(CypherAggregationUpdater.RELATIONSHIP_PROPERTIES) || projectionConfig
            .containsKey(CypherAggregationUpdater.RELATIONSHIP_TYPE) || projectionConfig.containsKey(
            CypherAggregationUpdater.ALPHA_RELATIONSHIP_PROPERTIES);

        var configAsAlphaParameter = migrationConfig != NoValue.NO_VALUE;

        if (containsRelationshipKeys || configAsAlphaParameter) {
            throw error(
                "The parameters for `nodesConfig` and `relationshipsConfig` have been merged. " +
                    "Update your query by merging the 4th and 5th parameter into one parameter."
            );
        }
    }

    private static void checkForNotMigratedConfigKeys(MapValue dataConfig) {
        if (dataConfig.containsKey(CypherAggregationUpdater.ALPHA_RELATIONSHIP_PROPERTIES)) {
            throw error(
                "The configuration key '%s' is now called '%s'.",
                CypherAggregationUpdater.ALPHA_RELATIONSHIP_PROPERTIES,
                CypherAggregationUpdater.RELATIONSHIP_PROPERTIES
            );
        }
    }

    private static void checkForMergedOrSwappedOrForgottenConfig(
        AnyValue projectionConfig,
        Collection<String> dataConfigKeys
    ) {
        if (dataConfigKeys.stream().anyMatch(PROJECTION_CONFIG_KEYS::contains)) {
            checkForSwappedOrForgottenConfig(projectionConfig, dataConfigKeys);
            checkForMergedConfig(dataConfigKeys);
        }
    }

    private static void checkForSwappedOrForgottenConfig(
        AnyValue projectionConfig,
        Collection<String> dataConfigKeys
    ) {
        if (PROJECTION_CONFIG_KEYS.containsAll(dataConfigKeys)) {
            if (projectionConfig == NoValue.NO_VALUE) {
                throw error(
                    "The `dataConfig` configuration parameter is missing. " +
                        "If you meant to provide an empty configuration for the 4th parameter, " +
                        "you can pass an empty map: '{}'."
                );
            } else {
                throw error(
                    "The configuration parameters are provided in the wrong order. " +
                        "Update your query by swapping the 4th and 5th parameter."
                );
            }
        }
    }

    private static void checkForMergedConfig(Collection<String> dataConfigKeys) {
        if (dataConfigKeys.stream().anyMatch(DATA_CONFIG_KEYS::contains)) {
            throw error(
                "The configuration parameters are merged and provided as one parameter. " +
                    "Update your query by splitting the configuration into two parameters. " +
                    "Refer to the documentation for details."
            );
        }
    }

    private static void checkForMutuallyRequiredKeys(MapValue dataConfig, String firstKey, String secondKey) {
        if (dataConfig.containsKey(firstKey) && !dataConfig.containsKey(secondKey)) {
            throw error(
                "The configuration key '%1$s' is missing, but '%2$s' is provided. " +
                    "If you really meant to only provide `%2$s` with no value for `%1$s`, " +
                    "you can set `%1$s` to `NULL`.",
                secondKey,
                firstKey
            );
        }
    }

    private static IllegalArgumentException error(@PrintFormat String message, Object... args) {
        return new IllegalArgumentException(formatWithLocale(message, args));
    }

    private static Collection<String> mapKeys(MapValue map) {
        var keys = map.keySet();
        if (keys instanceof Collection) {
            return (Collection<String>) keys;
        }
        return StreamSupport.stream(keys.spliterator(), false).collect(Collectors.toSet());
    }
}
