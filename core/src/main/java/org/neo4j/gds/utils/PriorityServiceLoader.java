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
package org.neo4j.gds.utils;

import java.util.Comparator;
import java.util.function.Function;

import static java.util.ServiceLoader.Provider;
import static java.util.ServiceLoader.load;

public final class PriorityServiceLoader {

    private PriorityServiceLoader() {}

    public static <T> T loadService(Class<T> serviceClass, Function<T, Integer> priorityFunction) {
        return load(serviceClass, serviceClass.getClassLoader())
            .stream()
            .map(Provider::get)
            .max(Comparator.comparing(priorityFunction))
            .orElseThrow(() -> new LinkageError("Could not load " + serviceClass + " implementation"));
    }
}
