/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public class EncryptUtil
{
    private static final String SECRET_KEY = "F3M0PxSWod6cyCejYUkpccU9gMsWwgrM";
    private static final String SALT = "G2PuhRinJqKKFcBUT4eMaK3FKMx9iGmx";

    private static final Logger logger = LoggerFactory.getLogger(EncryptUtil.class);

    private static IvParameterSpec ivSpec;
    private static SecretKeySpec secSpec;
    private static Cipher pbeCipher;

    static {
        try {
            final int iterationCount = 40000;
            final int keyLength = 128;
            byte[] iv = new byte[16];
            new SecureRandom().nextBytes(iv);
            ivSpec = new IvParameterSpec(iv);
            pbeCipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(SECRET_KEY.toCharArray(), SALT.getBytes(StandardCharsets.UTF_8), iterationCount, keyLength);
            SecretKey keyTmp = keyFactory.generateSecret(keySpec);
            secSpec = new SecretKeySpec(keyTmp.getEncoded(), "AES");
        }
        catch (Exception e) {
            logger.error("Unknown checked exception occurred: ", e);
        }
    }

    public static String encrypt(String password)
    {
        try {
            pbeCipher.init(Cipher.ENCRYPT_MODE, secSpec, ivSpec);
            byte[] cryptoText = pbeCipher.doFinal(password.getBytes(StandardCharsets.UTF_8));
            return base64Encode(cryptoText);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String base64Encode(byte[] bytes)
    {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String decrypt(String encrypted)
    {
        try {
            pbeCipher.init(Cipher.DECRYPT_MODE, secSpec, ivSpec);
            return new String(pbeCipher.doFinal(base64Decode(encrypted)), StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] base64Decode(String property)
    {
        return Base64.getDecoder().decode(property);
    }

    // generate encrypt password string which paste to json file
    public static void main(String[] args)
    {
        if (args.length != 1) {
            System.out.println("Usage: java -jar addax-common.jar <password>");
            System.exit(1);
        }
        String encrypted = encrypt(args[0]);
        System.out.println("The encrypt string is : '${enc:" + encrypted + "}', you can paste it to json file.");
    }
}
