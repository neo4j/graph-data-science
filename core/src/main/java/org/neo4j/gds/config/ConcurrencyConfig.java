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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface ConcurrencyConfig {

    String CONCURRENCY_KEY = "concurrency";
    int DEFAULT_CONCURRENCY = 4;
    Concurrency TYPED_DEFAULT_CONCURRENCY = new Concurrency(DEFAULT_CONCURRENCY);
    int CONCURRENCY_LIMITATION = 4;

    @Value.Default
    @Configuration.Key(CONCURRENCY_KEY)
    @Configuration.ConvertWith(method = "org.neo4j.gds.config.ConcurrencyConfig#parse")
    @Configuration.ToMapValue("org.neo4j.gds.config.ConcurrencyConfig#render")
    default Concurrency concurrency() {
        return TYPED_DEFAULT_CONCURRENCY;
    }

    static Concurrency parse(Object userInput) {
        if (userInput instanceof Concurrency) return (Concurrency) userInput;
        if (userInput instanceof Integer) return new Concurrency((Integer) userInput);
        if (userInput instanceof Long) return new Concurrency(Math.toIntExact((Long) userInput));
        throw new IllegalArgumentException(
            formatWithLocale(
                "Unsupported Concurrency input of type %s",
                userInput.getClass().getSimpleName()
            )
        );
    }

    static int render(Concurrency concurrency) {
        return concurrency.value();
    }

    @Configuration.Check
    default void validateConcurrency() {
        ConcurrencyValidatorService.validator().validate(concurrency().value(), CONCURRENCY_KEY, CONCURRENCY_LIMITATION);
    }

}
