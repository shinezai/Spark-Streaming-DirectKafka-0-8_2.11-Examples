package utils;

import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.Charset;

/**
 * Created by Administrator on 2017/5/8.
 */
public class MD5Utils {
    /** When we encode strings, we always specify UTF8 encoding */
    private static final String UTF8_ENCODING = "UTF-8";

    /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);

    /**
     *
     * @param str
     * @return
     */
    @SuppressWarnings("Since15")
    public static String md5_32bit(String str){
        return MD5Hash.getMD5AsHex(str.getBytes(UTF8_CHARSET));
    }

    public static String md5_16bit(String str){
        return md5_32bit(str).substring(8,24);
    }
}
