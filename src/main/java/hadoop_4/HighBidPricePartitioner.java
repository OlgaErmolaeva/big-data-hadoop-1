package hadoop_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HighBidPricePartitioner extends Partitioner<PairWritable, IntWritable>{

    @Override
    public int getPartition(PairWritable pairWritable, IntWritable intWritable, int numPartitions) {
        return pairWritable.getOperatingSystem().hashCode() * 127 % numPartitions;
    }
}
