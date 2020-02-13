package com.dnastack.ga4gh.search.adapter.data;

import com.nimbusds.jose.JOSEException;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class EncryptionServiceTest {


    public static final String PRIVATE_KEY_A = "-----BEGIN RSA PRIVATE KEY-----\n"
        + "MIIEpQIBAAKCAQEAwRZMmmotzlzd/M1sGfpcTK8E3XarPOH/OOv1bbGL/2tu3Ukf\n"
        + "aahikY2MPt39wBFDLyMXI16EwuensRt0uFHJpKTGQaeGRbbc02XWGGcaqBMglv1U\n"
        + "DKIqTP2eCKhAAnbA4GA9o8OZA3kcdXzMn+In2RQT7wCF4OwLwnH8nB3rPSORb0kW\n"
        + "av3enFehJy9MlFsvKs8nv82+tyutxeIiIEnwqMyOFIL0P4uWAjgXx18W5KPwCATI\n"
        + "Gf8FQTKZYUaFX8cn+jqJrPHkbENukqrBPInxyyOmIpWSQhagQLWLsz1BYbPh6bNm\n"
        + "6LX8YFNuRhinWzSJ8ak7QEbd1a+/DlA0P9BCnwIDAQABAoIBABffkPMao7U7KDtE\n"
        + "jtQbb2QP07MqI/vwGWVJT1wTsrKO7vMvQlIG/sDuVmPDgMdJmxvt48N8wT9z8vjj\n"
        + "4yeS/TJ9KQIfG2vtMxp0Ied8gAA99S6V/dleB4rjZLG/U8K6OhjD7XsdCQMz6z/f\n"
        + "gVFXXgPqBpjrYKIxrTaKb2hCDr7NOwRtPUBiiOvO+a6QU8Csvb96xHuM1kDGJyax\n"
        + "2din3p/quVBQTDIKl+XuZlESnbLQ0py39cK5QDZDML55viH60AzLxoY9cyOVmeCh\n"
        + "//Zj77EQK4MbPNShtTKR3K4m2+OHwWiieunB4cG+JJwvTCOX52/KOuQbQIOmEljG\n"
        + "xe0SKRECgYEA49UvOuUjgWkWv0mi2Nz8EVUjrOWcP6fL2aofVNDbucJHcLLbf1Nj\n"
        + "sLDr8x5Jw+5fR5paHbMm8mC87X9zR3KevXyKOjcE0xnLDaD7EwDew4fKfGOETts3\n"
        + "7bh6WpYtvz42NoWGd9a/XTd6TM3gDrdWCV4PD7dS4LYjM4rOa/YozccCgYEA2PVt\n"
        + "z5etpmQRehigsXsjpQv2W2gx4bTYzSmpcna1u5E1DNtlKHTFjEVwyJU2mVaMRCp7\n"
        + "OqKF9wblWnIAVnutO6aLVhAqQHVLT+hu7WApfOYOzlagyxlStpE7wKw5XjyGI27r\n"
        + "tUdUsKyQQ/2cU2w7asFcGXP4fKsT7BFDFEY9RGkCgYEAs6SV0A961oBhQkUylWJx\n"
        + "ks0aCysk74BEGQWuHLdueWL555/vuM6szVZDRXh9W3NqR1AqfD6VmAm1DmMvJ4f2\n"
        + "KJy5dTuKn1U4KS/kqNBH1c8NVtU1LuRljhimySSfP6szHfxc0cNPMpYa5dJzN8dR\n"
        + "nKNApvi67ZpP2UU6jCAVfmMCgYEAsKCON5T6UB3jhI0MNbFUQYfNlnjrMx8x6UTj\n"
        + "qEYwiXUbxU3PuvHl6d6WhRXFD2zAYLZdAE1T1toMWme5Eq85Rvi9MuGSJEvZdkJ6\n"
        + "HFJPNFlu8FjiKraA+hSemqsbQTbw1WP2DOz3a6XxBQkw/VF9xdmJC0e4onX8kBE1\n"
        + "lQnh0xECgYEAo/MnbAxiZdPNWnIJ5pcEEKx40KmeE+H1uLDANx0oaWVnp4qDJE7c\n"
        + "zKhZ8hax6T17iBAyVATU+Vw4kAAcajHGomn/eCbQH9cwhbAkTWJRtycAYTrXydUN\n"
        + "gnP8Y9vXNUSnnakvaXK+Bjt6G6xitaxlCYr14IQmsA3dZ4YF+bz4g1M=\n"
        + "-----END RSA PRIVATE KEY-----\n";

    public static final String PRIVATE_KEY_B = "-----BEGIN RSA PRIVATE KEY-----\n"
        + "MIIEpgIBAAKCAQEAyr+Irb3jzX5YLicec8l1NXu4URlfq8nmZJiFcVrFqVkiCyzK\n"
        + "TfbDFME1XmGHVNYk9V48AmvPJ1wEaLLRpbv/dzVj/qTHe3EVAhJ42IfkdulmCCrs\n"
        + "tN6Db6f/2TB7pOMjeHA7nZNYYAh4fH7xT7wo4JT/s9hiyziDeUzMtkv1pf0GpulI\n"
        + "ZIUPUGR/098MAh6YBzGMEAnJaIvmhu68RNcVgGLSUy0Xm8Mu6ksqmdIJplh9eLe/\n"
        + "6TinMy57doFQYqSQsdeboF+olTX9ecPJR86PXNWS9eOXRbCvIdFolNrYGZcYApcN\n"
        + "WTxunZjxl6wNgB/ZL6gnHU81xkDJdUdyytsg2QIDAQABAoIBAQDDymL7JT0v3Krb\n"
        + "JvvJZOrDU06TQqWZB8htrLxw0u++skyl7gMPdxS8njgjhJtj2cinCvF4hRPvNnUr\n"
        + "hkPegMYbIHwIFKhvFKzs61m4AMMn8gyOJqWfpKHY2avA2YkNmitOy1NGwFu/oW55\n"
        + "Cz2Nem8fMcQcsQH2rgSIxxmJkaEPQKjRuSXn9V/TIPWWN0tpQCq6sk0lWPXe1zkp\n"
        + "Z7zMV23aItONcoB2DisTkp+GW0WuhjzccmLc0BmkWocR25H1U5k6aSyPTAMS5YU+\n"
        + "buu3QoX96SkqkQxDpyIAr0Vr8hv9Kgvs0J6tJOPzWyiAR20f7BUpxYcBmhU06kkG\n"
        + "FiyMpqwBAoGBAPDUY6BNOxScx7SWCk2fOh7w60Ee1COWaFaiymFo4V525UuJhn7M\n"
        + "nQR6FDh/xPAwdHyN9xzwxOuWaqCIaTAKEAmkQeeONwyNvFoFa20dLHrY9ZW1W3wr\n"
        + "jkHKDnIBRgZUAD6/hIDUnDlGUJGbHoaBVtsYO4GU9YhMK0DXY9zET7LRAoGBANeF\n"
        + "C1ZYWgfevsTetIn1/JCqdorKOTG/DuqX/srO7xrL8FlisGkTbypVjy3NUHTnEfDr\n"
        + "mM3iFE84fzSt0QxgUvjWwdi7oCuM3FYAQMLJl7QlMWkoWYtnlzEGRiA+P/jEwX6o\n"
        + "mYpseIfTWsWuHMuqczfMTlJ3z1frzJpAXzDKwz+JAoGBAOHUWAPGyCMBDWfy1oCJ\n"
        + "UkgXZJNxJD6m2Pp8tsh3P/aECWo28O7QUmETVQjSD0uoNGNkAJib0hyUtTIKGJEa\n"
        + "roVMCRzoyAg2lKvZry3nkIE2F7ymDr62XTSmE7umJBLlDEHK96aTOFeg7wZK7mn0\n"
        + "uQzEgXB2zXNc8aZhE9y+HXoxAoGBANFXL/387+Z5HEBKOn9ParZyfZauewOxG6IP\n"
        + "ZbRFcMdMlPed/tDxyacLWHoa1i3XTPwBPHmvQh75N8ZJl6c34SBIKh6FkTz8JAF9\n"
        + "WWpbOnoHLomAYgog4yEBgRhMnwLay0BmME3esuzsBV3Ck8HYnPX0A5fjPImgRysF\n"
        + "n/Da4vBRAoGBAOL8Q5JArHiwx52JvPuFhz0NVmHSGshuRNYLXuQY33oal+f4YcuF\n"
        + "qngjOpEaCz5MiL6ABH7UgllKWC1yW8XLbQBrlfzdEJrbIQcEUa/NnB65HvUBzXFv\n"
        + "eMwhYiChLhgSeaqWw7h2c/M8Wf/9+/2bTIRnrK9z3eEqKiRYA6ThMVrH\n"
        + "-----END RSA PRIVATE KEY-----\n";


    @Test
    public void encryptThenDecrypt() throws JOSEException, ParseException {
        EncryptionService encryptionService = new EncryptionService(PRIVATE_KEY_A);
        String data = "Hello World!";
        String encrypted = encryptionService.encrypt(data);
        Assert.assertNotEquals(data, encrypted);
        String decrypted = encryptionService.decrypt(encrypted);
        Assert.assertEquals(data, decrypted);
    }

    @Test(expected = JOSEException.class)
    public void encryptWithKeyAThenDecryptWithKeyBFails() throws JOSEException, ParseException {
        EncryptionService encryptionService = new EncryptionService(PRIVATE_KEY_A);
        String data = "Hello World!";
        String encrypted = encryptionService.encrypt(data);
        Assert.assertNotEquals(data, encrypted);

        EncryptionService decryptionService = new EncryptionService(PRIVATE_KEY_B);
        decryptionService.decrypt(encrypted);
    }

}