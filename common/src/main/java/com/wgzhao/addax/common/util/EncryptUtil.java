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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

import static com.wgzhao.addax.common.base.Constant.ENC_PASSWORD_PREFIX;

public class EncryptUtil
{
    private static final String SECRET_KEY = "F3M0PxSWod6cyCejYUkpccU9gMsWwgrM";
    private static final byte[] SALT = "G2PuhRinJqKKFcBUT4eMaK3FKMx9iGmx".getBytes();

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String ALGORITHM = "AES";

    private static final Logger logger = LoggerFactory.getLogger(EncryptUtil.class);


    public static String encrypt(String password)
    {
        try {
            SecretKeySpec secSpec = getSecSpec();
            GCMParameterSpec params = new GCMParameterSpec(128, SECRET_KEY.getBytes(), 0, 12);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secSpec, params);
            byte[] cryptoText = cipher.doFinal(password.getBytes());
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
            SecretKeySpec secSpec = getSecSpec();
            GCMParameterSpec params = new GCMParameterSpec(128, SECRET_KEY.getBytes(), 0, 12);
            Cipher pbeCipher = Cipher.getInstance(TRANSFORMATION);
            pbeCipher.init(Cipher.DECRYPT_MODE, secSpec, params);
            return new String(pbeCipher.doFinal(base64Decode(encrypted)));
        } catch (InvalidAlgorithmParameterException | NoSuchPaddingException | IllegalBlockSizeException |
                 InvalidKeySpecException | NoSuchAlgorithmException | BadPaddingException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }

    }

    private static byte[] base64Decode(String property)
    {
        return Base64.getDecoder().decode(property);
    }

    private static SecretKeySpec getSecSpec() throws InvalidKeySpecException, NoSuchAlgorithmException {
        final int iterationCount = 40000;
        final int keyLength = 128;
        byte[] iv = new byte[16];
        new SecureRandom().nextBytes(iv);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        PBEKeySpec keySpec = new PBEKeySpec(SECRET_KEY.toCharArray(), SALT, iterationCount, keyLength);
        SecretKey keyTmp = keyFactory.generateSecret(keySpec);
        return  new SecretKeySpec(keyTmp.getEncoded(), ALGORITHM);
    }

    // generate encrypt password string which paste to json file
    public static void main(String[] args)
    {
        if (args.length != 1) {
            System.out.println("Usage: java -jar addax-common.jar <password>");
            System.exit(1);
        }
        String encrypted = encrypt(args[0]);
        System.out.printf("The encrypt string is: '%s%s}', you can paste it into json file.", ENC_PASSWORD_PREFIX, encrypted);
    }
}
