package com.lin.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author linzj8
 */
public class NetUtil {

    private final static Pattern IP_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
    public static String getLocalIp(String ipPreference) {
        if (ipPreference == null) {
            ipPreference = "*>10>172>192>127";
        }
        String[] prefix = ipPreference.split("[> ]+");
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            String matchIp = null;
            int matchIdx = -1;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();
                // 跳过虚拟网卡
                if (ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> ia = ni.getInetAddresses();
                // 跳过虚拟网卡
                while (ia.hasMoreElements()) {
                    InetAddress inetAddress = ia.nextElement();
                    if (inetAddress.isLoopbackAddress() || !inetAddress.isSiteLocalAddress()
                            || inetAddress.isAnyLocalAddress()) {
                        continue;
                    }
                    String ip = inetAddress.getHostAddress();
                    Matcher matcher = IP_PATTERN.matcher(ip);
                    if (matcher.matches()) {
                        int index = matchedIndex(ip, prefix);
                        if (index == -1) {
                            continue;
                        }
                        if (matchIdx == -1) {
                            matchIdx = index;
                            matchIp = ip;
                        } else {
                            if (matchIdx > index) {
                                matchIdx = index;
                                matchIp = ip;
                            }
                        }
                    }
                }
            }
            if (matchIp != null) {
                return matchIp;
            }
            return "127.0.0.1";
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    public static String getLocalIp() {
        return getLocalIp("*>10>172>192>127");
    }

    public static int matchedIndex(String ip, String[] prefix) {
        for (int i = 0; i < prefix.length; i++) {
            String p = prefix[i];
            if ("*".equals(p)) {
                if (ip.startsWith("127.") || ip.startsWith("10.") || ip.startsWith("172.") || ip.startsWith("192.")) {
                    continue;
                }
                return i;
            } else {
                if (ip.startsWith(p)) {
                    return i;
                }
            }
        }
        return -1;
    }
}
