package com.alan;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@SuppressWarnings("deprecation")
public class RevertString extends UDF {
    public Text evaluate(Text line) {
        if (null != line && !line.toString().equals("")) {
            String str = line.toString();
            char[] rs = new char[str.length()];
            for (int i = str.length() - 1; i >= 0; i--) {
                rs[str.length() - 1 - i] = str.charAt(i);
            }
            line.set(new String(rs));
            return line;
        }
        line.set("");
        return line;
    }
}
