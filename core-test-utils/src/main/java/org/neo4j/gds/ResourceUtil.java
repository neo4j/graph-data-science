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
package org.neo4j.gds;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ResourceUtil {

    public static List<String> lines(String resourceName) {
        var resourcePath = path(resourceName);
        try {
            return Files.readAllLines(resourcePath, UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path path(String resourceName) {
        var classLoader = Objects.requireNonNullElse(
            Thread.currentThread().getContextClassLoader(),
            ResourceUtil.class.getClassLoader()
        );

        var resourceUrl = classLoader.getResource(resourceName);
        Objects.requireNonNull(resourceUrl, () -> String.format(Locale.ENGLISH, "The resource %s cannot be found.", resourceName));

        final URI resourceUri;
        try {
            resourceUri = resourceUrl.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return Paths.get(resourceUri);
    }

    private ResourceUtil() {}
}
