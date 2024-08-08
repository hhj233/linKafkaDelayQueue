package com.lin.common.util;

/**
 * @author linzj
 */
public class HashUtil {

    /**
     * FNV1_32_HASH算法
     * @param str
     * @return
     */
    public static Integer FNV1_32_HASH(String str) {
        final int p = 16777769;
        int hash = 0;
        int length = str.length();
        for(int i = 0; i < length; i++) {
            hash = (hash^str.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }


}
