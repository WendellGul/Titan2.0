package com.thinkaurelius.titan.diskstorage.impala.test;

import java.util.Arrays;

/**
 * Created by Great on 2017/3/31.
 */
public class AnyTest {
    public static void main(String[] args) {
        byte[] b = { 0x11, 0x22 };
        System.out.println("0x" + bytesToHexString(b));
    }

    public static String bytesToHexString(byte[] b) {
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < b.length; i++) {
            sb.append(Integer.toHexString(b[i]));
        }
        return sb.toString();
    }
}
