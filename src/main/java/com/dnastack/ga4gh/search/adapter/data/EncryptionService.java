package com.dnastack.ga4gh.search.adapter.data;


import com.dnastack.ga4gh.search.adapter.security.RsaKeyHelper;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.RSADecrypter;
import com.nimbusds.jose.crypto.RSAEncrypter;
import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import lombok.NonNull;

/**
 * Simple encryption service for protecting query contents when persisted. The Raw strings are encrypted in the <a
 * href="https://tools.ietf.org/html/rfc7516">JWE Format</a>
 */
public class EncryptionService {

    private final RSAEncrypter rsaEncrypter;
    private final RSADecrypter rsaDecrypter;

    /**
     * Given an RSA Private Key String in the PEM format, create the encryption service using the Public key for
     * encryption and the Private key for decryption.
     *
     * Example of how to generate the KEY
     * <code>
     * openssl genrsa -out rsa-key.pem 2048
     * </code>
     */
    public EncryptionService(@NonNull String rsaKey) {
        try {
            KeyPair keyPair = RsaKeyHelper.parseKeyPair(rsaKey);
            rsaDecrypter = new RSADecrypter(keyPair.getPrivate());
            rsaEncrypter = new RSAEncrypter((RSAPublicKey) keyPair.getPublic());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String encrypt(String content) throws JOSEException {
        JWEHeader header = new JWEHeader.Builder(JWEAlgorithm.RSA_OAEP_256, EncryptionMethod.A256GCM).contentType("JWE")
            .build();
        Payload payload = new Payload(content);
        JWEObject jweObject = new JWEObject(header, payload);
        jweObject.encrypt(rsaEncrypter);
        return jweObject.serialize();
    }


    public String decrypt(String content) throws JOSEException, ParseException {
        JWEObject jweObject = JWEObject.parse(content);
        jweObject.decrypt(rsaDecrypter);
        return jweObject.getPayload().toString();
    }

}
