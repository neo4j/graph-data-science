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
package org.neo4j.graphalgo.core;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import org.bouncycastle.util.io.pem.PemReader;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.ValueClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Date;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class SignatureTool {
    private static final String ENCRYPTION_ALGORITHM = "RSA";

    public static LicenseCheckResult verify(@NotNull String license) {
        try {
            PublicKey key = getPublicKey();
            Jws<Claims> token = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(license);
            Claims claims = token.getBody();

            Date now = new Date();
            if (!claims.getSubject().equals("neo4j-gds")) {
                return ImmutableLicenseCheckResult.of(
                    false,
                    "License is not valid for the Graph Data Science library. Please contact your system administrator."
                );
            } else {
                long daysLeft = (claims
                                     .getExpiration()
                                     .getTime() - now.getTime()) / 1000 / 60 / 60 / 24; // Divide to go from milliseconds to days
                return ImmutableLicenseCheckResult.of(true, formatWithLocale("License valid, %d days left", daysLeft));
            }
        } catch (ExpiredJwtException e) {
            return ImmutableLicenseCheckResult.of(
                false,
                formatWithLocale("License expired on %s", e.getClaims().getExpiration())
            );
        } catch (Exception e) {
            return ImmutableLicenseCheckResult.of(
                false,
                formatWithLocale("Could not validate license. Cause: %s", e.getMessage())
            );
        }
    }

    private static PublicKey getPublicKey() throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        InputStream inputStream = SignatureTool.class.getResourceAsStream("/signing-keys/public_key.pem");

        InputStreamReader streamReader = new InputStreamReader(
            inputStream,
            StandardCharsets.UTF_8
        );

        PemReader pemReader = new PemReader(streamReader);
        byte[] key = pemReader.readPemObject().getContent();

        KeyFactory keyFactory = KeyFactory.getInstance(ENCRYPTION_ALGORITHM);
        return keyFactory.generatePublic(new X509EncodedKeySpec(key));
    }

    @ValueClass
    public interface LicenseCheckResult {

        boolean isValid();

        String message();
    }

    private SignatureTool() {}
}
