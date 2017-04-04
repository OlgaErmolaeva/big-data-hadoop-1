package hadoop_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class BytesPerIPMapperTest {

    @Test
    public void mapTest() {

        Text value = new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"");

        new MapDriver<LongWritable, Text, Text, PairWritable>()
                .withMapper(new BytesPerIPMapper())
                .withInput(new LongWritable(0),value)
                .withOutput(new Text("ip1"),new PairWritable(1,40028))
                .runTest();
    }
}