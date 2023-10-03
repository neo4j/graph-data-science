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
package org.neo4j.gds.procedures.community;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitViolation;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;

public class ConfigurationParser {
    private final DefaultsConfiguration defaults;
    private final LimitsConfiguration limits;

    public static final ConfigurationParser  EMPTY = new ConfigurationParser(DefaultsConfiguration.Empty,LimitsConfiguration.Empty);

    public ConfigurationParser(
        DefaultsConfiguration defaults,
        LimitsConfiguration limits
    ) {

        this.defaults = defaults;
        this.limits = limits;
    }

    Map<String,Object> applyDefaults(Map<String, Object> configuration, String username) {
        // apply defaults
        return  defaults.apply(configuration, username);
    }


    <CONFIG  extends AlgoBaseConfig> void validateLimits(
        CONFIG algorithmConfiguration,
        String username,
        Map<String, Object> userInputWithDefaults
    ) throws  IllegalArgumentException{

        // handle limits
        var allowedKeys = new HashSet<>(algorithmConfiguration.configKeys());
        var irrelevantInputtedKeys = getIrrelevantInputtedKeys(userInputWithDefaults, allowedKeys);
        var configurationButWithIrrelevantInputtedKeysRemoved = getConfigurationForLimitValidation(
            userInputWithDefaults,
            irrelevantInputtedKeys
        ); //remove any useless configuration parameters e.g., sourceNode for Wcc

        validateLimits(configurationButWithIrrelevantInputtedKeysRemoved, username);

    }

    void validateOriginalConfig(
        Map<String, Object> configuration,
        Collection<String> allowedConfigKeys
    ) throws IllegalArgumentException {
        Map<String, Object> newConfiguration = new HashMap<>(configuration);

        CypherMapWrapper.create(newConfiguration)
            .requireOnlyKeysFrom(allowedConfigKeys); //ensure user has not included any  incorrect params
    }

    @NotNull
    private HashSet<String> getIrrelevantInputtedKeys(Map<String, Object> configuration, HashSet<String> allowedKeys) {
        var irrelevantInputtedKeys = new HashSet<>(configuration.keySet());
        irrelevantInputtedKeys.removeAll(allowedKeys);
        return irrelevantInputtedKeys;
    }

    @NotNull
    private HashMap<String, Object> getConfigurationForLimitValidation(
        Map<String, Object> configuration,
        HashSet<String> irrelevantInputtedKeys) {
        var configurationButWithIrrelevantInputtedKeysRemoved = new HashMap<>(configuration);
        configurationButWithIrrelevantInputtedKeysRemoved.keySet().removeAll(irrelevantInputtedKeys);
        return configurationButWithIrrelevantInputtedKeysRemoved;
    }

    private void validateLimits(HashMap<String, Object> configurationButWithIrrelevantInputtedKeysRemoved, String username) {
        var violations = limits.validate(configurationButWithIrrelevantInputtedKeysRemoved, username);

        if (violations.isEmpty()) return;

        var violationMessages = new LinkedList<String>();
        var delimeter = "\n";
        if (violations.size() > 1) {
            violationMessages.add("Configuration exceeded multiple limits:");
            delimeter = "\n - ";
        }

        violations.stream()
            .map(LimitViolation::getErrorMessage)
            .sorted()
            .forEach(violationMessages::add);

        throw new IllegalArgumentException(String.join(delimeter, violationMessages));
    }


    public <CONFIG extends AlgoBaseConfig> CONFIG produceConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, CONFIG> configCreator,
        String username
    ) {
        var inputWithDefaults = applyDefaults(configuration, username);
        var procConfig = configCreator.apply(CypherMapWrapper.create(inputWithDefaults));
        validateOriginalConfig(configuration, procConfig.configKeys());
        validateLimits(procConfig, username, inputWithDefaults);
        return procConfig;
    }

}
