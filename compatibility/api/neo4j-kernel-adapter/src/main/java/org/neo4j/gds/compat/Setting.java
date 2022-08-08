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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.gds.annotation.ValueClass;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.immutables.builder.Builder.Parameter;

@ValueClass
@Value.Style(build = "buildInner", depluralize = true)
public interface Setting<T> {

    @Parameter
    String name();

    @Parameter
    SettingValueParser<T> parser();

    @Parameter
    @Nullable
    T defaultValue();

    /**
     * Make this setting dynamic. A dynamic setting can be modified by the user through the 'dbms.setConfigValue' procedure.
     */
    @Value.Default
    default boolean dynamic() {
        return false;
    }

    /**
     * Make this setting immutable.
     */
    @Value.Default
    default boolean immutable() {
        return false;
    }

    List<SettingConstraint<T>> constraints();

    /**
     * A setting dependency is used to inherit missing parts of this
     * setting by whatever is set for the dependency setting.
     * For example, for relative paths, this could be the root directory;
     * for a socket addresses, it could be the host or the port.
     */
    Optional<org.neo4j.graphdb.config.Setting<T>> dependency();

    // consider this private
    Function<Setting<T>, org.neo4j.graphdb.config.Setting<T>> convert();

    interface Builder<T> {

        Builder<T> addConstraint(SettingConstraint<T> constraint);

        Builder<T> dependency(org.neo4j.graphdb.config.Setting<T> dependency);

        default Builder<T> dynamic() {
            return dynamic(true);
        }

        default Builder<T> immutable() {
            return immutable(true);
        }

        default org.neo4j.graphdb.config.Setting<T> build() {
            var setting = buildInner();
            return setting.convert().apply(setting);
        }

        Setting<T> buildInner();

        Builder<T> dynamic(boolean dynamic);

        Builder<T> immutable(boolean dynamic);
    }
}
