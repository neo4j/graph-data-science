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
package org.neo4j.graphalgo.core;

import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class SignatureTool {
    private static final String CHARSET = "UTF-8";
    private static final String ENCRYPTION_ALGORITHM = "RSA";
    private static final String HASH_ENCRYPTION_ALGORITHM = "SHA256withRSA";
    private static final String SIGNATURE_SEPARATOR = "======SIGNATURE======";

    private final Base64.Encoder encoder;
    private final Base64.Decoder decoder;

    public SignatureTool() {
        this.encoder = Base64.getEncoder();
        this.decoder = Base64.getDecoder();
    }

    public String sign(String message) throws
        NoSuchAlgorithmException,
        IOException,
        InvalidKeySpecException,
        InvalidKeyException, SignatureException {
        PrivateKey privateKey = getPrivateKey();

        Signature sign = Signature.getInstance(HASH_ENCRYPTION_ALGORITHM);
        sign.initSign(privateKey);
        sign.update(message.getBytes(CHARSET));
        ;
        var signatureString = encoder.encodeToString(sign.sign());

        var messageAndSignature = message + SIGNATURE_SEPARATOR + signatureString;

        return encoder.encodeToString(messageAndSignature.getBytes(CHARSET));
    }

    public boolean verify(String encodedMessageAndSignature) {
        try {
            String messageAndSignature = new String(decoder.decode(encodedMessageAndSignature), CHARSET);
            String[] messageAndSignatureList = messageAndSignature.split(SIGNATURE_SEPARATOR);

            String message = messageAndSignatureList[0];
            byte[] signature = decoder.decode(messageAndSignatureList[1]);

            Signature sign = Signature.getInstance(HASH_ENCRYPTION_ALGORITHM);
            sign.initVerify(getPublicKey());
            sign.update(message.getBytes(CHARSET));
            return sign.verify(signature);
        } catch (Exception e) {
            throw new RuntimeException("Verification of the License Key failed", e);
        }
    }


    private PrivateKey getPrivateKey() throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        InputStream inputStream = getClass().getResourceAsStream("/signing-keys/private_key.pem");
        byte[] key = readPem(inputStream);

        KeyFactory keyFactory = KeyFactory.getInstance( ENCRYPTION_ALGORITHM );
        return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(key));
    }

    private PublicKey getPublicKey() throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        InputStream inputStream = getClass().getResourceAsStream("/signing-keys/public_key.pem");

        byte[] key = readPem(inputStream);

        KeyFactory keyFactory = KeyFactory.getInstance( ENCRYPTION_ALGORITHM );
        return keyFactory.generatePublic(new X509EncodedKeySpec(key));
    }

    private byte[] readPem(InputStream inputStream) throws IOException {
        InputStreamReader streamReader = new InputStreamReader(
            inputStream,
            StandardCharsets.UTF_8
        );

        PemReader pemReader = new PemReader(streamReader);
        return pemReader.readPemObject().getContent();
    }

}
