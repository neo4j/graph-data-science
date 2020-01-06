/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package positive;

import org.neo4j.graphalgo.annotation.Configuration;

import java.util.Collections;
import java.util.Map;

public interface Ignores {

    public interface BaseConfig {
        double canAlsoIgnoredInheritedMethods();

        long notIgnored();
    }

    @Configuration("MyConfig")
    public interface MyConfig extends BaseConfig {

        @Configuration.Ignore
        default String canIgnoreAnyMethod() {
            return "foo";
        }

        @Configuration.Ignore
        @Override
        default double canAlsoIgnoredInheritedMethods() {
            return 42;
        }

        static int staticMethodsAreAlsoIgnored() {
            return 1337;
        }

        @Configuration.Ignore
        @Configuration.Parameter
        default String canIgnoreParametersAsWell() {
            return "foo";
        }

        @Configuration.Ignore
        @Configuration.Key("bar")
        default String canIgnoreKeyAnnotationsAsWell() {
            return "baz";
        }

        @Configuration.Ignore
        default char canIgnoreInvalidMethods() {
            return 's';
        }

        @Configuration.Ignore
        default void canIgnoreInvalidMethods2() {}

        @Configuration.Ignore
        default int[] canIgnoreInvalidMethods3() {
            return new int[42];
        }

        @Configuration.Ignore
        default String canIgnoreInvalidMethods4(boolean nope) {
            return "foo";
        }

        @Configuration.Ignore
        default <A> A canIgnoreInvalidMethods5() {
            return null;
        }

        @Configuration.Ignore
        default <V> Map<String, V> canIgnoreInvalidMethods6() {
            return Collections.emptyMap();
        }

        @Configuration.Ignore
        default int canIgnoreInvalidMethods7() throws Exception {
            return 42;
        }

        @Configuration.Ignore
        default String canIgnoreInvalidMethods8() throws IllegalArgumentException {
            return "bar";
        }
    }
}
