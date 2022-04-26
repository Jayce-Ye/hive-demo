package cn.itcast.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @description: hive自定义函数UDF 实现对手机号中间4位进行****加密
 * @author: Itcast
 */
public class EncryptPhoneNumber extends GenericUDF {
    @Override
    //这个方法只调用一次，并且在evaluate()方法之前调用，该方法接收的参数是一个ObjectInspectors数组，该方法检查接收正确的参数类型和参数个数
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        return null;
    }

    //这个方法类似evaluate方法，处理真实的参数，返回最终结果
    @Override
    public String  evaluate(DeferredObject[] arguments) throws HiveException {
        String encryptPhoNum = null;
        String phoNum = arguments[0].get().toString();
        //手机号不为空 并且为11位
        if (StringUtils.isNotEmpty(phoNum) && phoNum.trim().length() == 11 ) {
            //判断数据是否满足中国大陆手机号码规范
            String regex = "^(1[3-9]\\d{9}$)";
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(phoNum);
            if (m.matches()) {//进入这里都是符合手机号规则的
                //使用正则替换 返回加密后数据
                encryptPhoNum = phoNum.trim().replaceAll("(\\d{3})\\d{4}(\\d{4})","$1****$2");
            }else{
                //不符合手机号规则 数据直接原封不动返回
                encryptPhoNum = phoNum;
            }
        }else{
            //不符合11位 数据直接原封不动返回
            encryptPhoNum = phoNum;
        }
        return encryptPhoNum;
    }

    //此方法用于当实现的GenericUDF出错的时候，打印提示信息，提示信息就是该方法的返回的字符串
    @Override
    public String getDisplayString(String[] children) {
        return "Usage:EncryptPhoneNumber(String str)";
    }
//    /**
//     * 重载evaluate方法 实现函数的业务逻辑
//     * @param phoNum  入参：未加密手机号
//     * @return 返回：加密后的手机号字符串
//     */
//    public String evaluate(String phoNum){
//        String encryptPhoNum = null;
//        //手机号不为空 并且为11位
//        if (StringUtils.isNotEmpty(phoNum) && phoNum.trim().length() == 11 ) {
//            //判断数据是否满足中国大陆手机号码规范
//            String regex = "^(1[3-9]\\d{9}$)";
//            Pattern p = Pattern.compile(regex);
//            Matcher m = p.matcher(phoNum);
//            if (m.matches()) {//进入这里都是符合手机号规则的
//                //使用正则替换 返回加密后数据
//                encryptPhoNum = phoNum.trim().replaceAll("(\\d{3})\\d{4}(\\d{4})","$1****$2");
//            }else{
//                //不符合手机号规则 数据直接原封不动返回
//                encryptPhoNum = phoNum;
//            }
//        }else{
//            //不符合11位 数据直接原封不动返回
//            encryptPhoNum = phoNum;
//        }
//        return encryptPhoNum;
//    }
}
