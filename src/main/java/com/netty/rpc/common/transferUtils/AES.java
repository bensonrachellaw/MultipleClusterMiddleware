package com.netty.rpc.common.transferUtils; /**
 * Created  on 2016/12/10.
 */

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class AES {

    private static final String ALGORITHM = "AES";
    private static final int KEY_SIZE = 128;
    private static final int CACHE_SIZE = 1024;

    public static String getSecretKey() throws NoSuchAlgorithmException {
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed("l".getBytes());
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        kgen.init(128, random);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        return Base64.getEncoder().encodeToString(enCodeFormat);
    }

    public static byte[] encrypt(byte[] data, String key) throws Exception {
        byte[] raw = Base64.getDecoder().decode(key);
        SecretKeySpec secretKeySpec = new SecretKeySpec(raw, ALGORITHM);
        String iv = "aabbccddeeffgghh";
        IvParameterSpec ivSpec = new IvParameterSpec(iv.getBytes());
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivSpec);
        return cipher.doFinal(data);
    }

    public static byte[] decrypt(byte[] data, String key) throws Exception {
        byte[] raw = Base64.getDecoder().decode(key);
        SecretKeySpec secretKeySpec = new SecretKeySpec(raw, ALGORITHM);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = new IvParameterSpec("aabbccddeeffgghh".getBytes());
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, iv);
        return cipher.doFinal(data);
    }

    public static void main(String[] args) throws Exception {
        String Key = "sdfs";
        Key = getSecretKey();
        byte[] b1 = {'a', 'b', 3, 6, 5, 4, 1, 2, 5, 6, 3, 6, 2, 1, 5, 4};
        for (int i = 0; i < b1.length; i++) {
            System.out.print(b1[i]);
        }
        b1 = encrypt(b1, Key);
        System.out.println();
        b1 = decrypt(b1, Key);
        for (int i = 0; i < b1.length; i++) {
            System.out.print(b1[i]);
        }
    }
}
