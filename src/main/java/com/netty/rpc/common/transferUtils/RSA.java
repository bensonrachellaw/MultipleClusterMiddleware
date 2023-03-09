package com.netty.rpc.common.transferUtils; /**
 * Created  on 2016/12/10.
 */

import javax.crypto.Cipher;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class RSA {

    private static final String PUBLIC_KEY = "RSAPublicKey";
    private static final String PRIVATE_KEY = "RSAPrivateKey";
    private Map<String, Object> mKeyMap;

    public RSA() {
        try {
            this.initKey();
        } catch (Exception var2) {
            var2.printStackTrace();
        }
    }

    public static byte[] encrypt(byte[] data, String key) throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(key);
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(keySpec);
        Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
        cipher.init(1, publicKey);
        return cipher.doFinal(data);
    }

    public static byte[] decrypt(byte[] data, String key) throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(key);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
        cipher.init(2, privateKey);
        return cipher.doFinal(data);
    }

    private void initKey() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
        keyPairGen.initialize(1024, new SecureRandom());
        KeyPair keyPair = keyPairGen.generateKeyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        this.mKeyMap = new HashMap(2);
        this.mKeyMap.put("RSAPublicKey", publicKey);
        this.mKeyMap.put("RSAPrivateKey", privateKey);
    }

    public String getPublicKey() {
        Key key = (Key) this.mKeyMap.get("RSAPublicKey");
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    public String getPrivateKey() {
        Key key = (Key) this.mKeyMap.get("RSAPrivateKey");
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }
}
