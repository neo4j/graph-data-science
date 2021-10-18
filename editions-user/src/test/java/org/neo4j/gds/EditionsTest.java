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

import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

public class EditionsTest {

    /*
    static {
        ServiceLoader<String> load = ServiceLoader.load(GdsEditionFactory.class);
        String s = load.findFirst().get();
        /*
        opengds         dependsOn { open-gds-string-provider }
        commercial      dependsOn { commercial-gds-string-provider }
    }
     */
    @Test
    void findFirst() {
        var newThingFactory = ServiceLoader.load(NewThingFactory.class)
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load " + NewThingFactory.class + " implementation"));
        System.out.println("first: " + newThingFactory.createEditionStuff().label());
    }

    @Test
    void findAll() {
        ServiceLoader.load(NewThingFactory.class).stream()
            .map(factory -> factory.get().createEditionStuff())
            .forEach(edition -> System.out.printf("---%nname: %s%nerrors: %s%n---%n", edition.label(), edition.errorMessage()));
    }

}
