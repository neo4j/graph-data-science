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

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Optional;

public final class SettingsUtil {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    public static Optional<Setting<?>> tryFind(
        String className,
        String settingName
    ) {
        try {
            var settingsClass = Class.forName(className);
            var settingHandle = LOOKUP.findStaticGetter(settingsClass, settingName, Setting.class);
            var setting = (Setting<?>) settingHandle.invoke();
            return Optional.ofNullable(setting);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            // Setting is not available on this version
            return Optional.empty();
        } catch (Throwable e) {
            // Setting exists but accessing it failed
            throw new IllegalStateException(String.format(
                Locale.ENGLISH,
                "The %s setting could not be found: %s",
                settingName,
                e.getMessage()
            ), e);
        }
    }

    public static <T> Optional<T> tryGet(
        Configuration config,
        Class<T> settingType,
        Setting<?> setting
    ) {
        var settingValue = config.get(setting);
        if (settingValue == null) {
            return Optional.empty();
        }
        return Optional.of(settingType.cast(settingValue));
    }

    public static <T> Optional<T> tryGet(
        Configuration config,
        Class<T> settingType,
        String className,
        String settingName
    ) {
        return tryFind(className, settingName).flatMap(setting -> tryGet(config, settingType, setting));
    }

    public static <T, U> T tryConfigure(
        T builder,
        SetConfig<T, U> setConfig,
        String className,
        String settingName,
        U settingValue
    ) {
        return tryFind(className, settingName).map(setting -> {
            //noinspection unchecked
            var typedSetting = (Setting<U>) setting;
            return setConfig.set(builder, typedSetting, settingValue);
        }).orElse(builder);
    }

    public interface SetConfig<T, S> {
        T set(T config, Setting<S> setting, S value);
    }

    private SettingsUtil() {
        throw new UnsupportedOperationException();
    }
}
