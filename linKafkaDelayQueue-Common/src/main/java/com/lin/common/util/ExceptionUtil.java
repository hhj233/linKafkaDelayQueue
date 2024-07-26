package com.lin.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author linzj
 */
public class ExceptionUtil {

    public static String getStackTraceAsString(Throwable e) {
        if (e == null){
            return "";
        }
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
