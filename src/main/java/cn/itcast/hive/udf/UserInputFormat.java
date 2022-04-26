package cn.itcast.hive.udf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @ClassName UserInputFormat
 * @Description TODO 用于实现自定义InputFormat，读取每行数据
 * @Create By     Itcast
 */

public class UserInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job,
                                                            Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        UserRecordReader reader = new UserRecordReader(job,(FileSplit)genericSplit);
        return reader;
    }
}