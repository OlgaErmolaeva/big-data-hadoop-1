package hadoop_3;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PairWritable implements WritableComparable<PairWritable> {

    private IntWritable amountOfIp;
    private LongWritable totalByte;

    public PairWritable() {
        set(new IntWritable(), new LongWritable());
    }

    public PairWritable(int amountOfIp, long totalByte) {
        set(new IntWritable(amountOfIp), new LongWritable(totalByte));
    }

    public PairWritable(IntWritable amountOfIp, LongWritable totalByte) {
        set(amountOfIp, totalByte);
    }

    public void set(IntWritable amountOfIp, LongWritable totalByte) {
        this.amountOfIp = amountOfIp;
        this.totalByte = totalByte;
    }

    public IntWritable getAmountOfIp() {
        return amountOfIp;
    }

    public LongWritable  getTotalByte() {
        return totalByte;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        amountOfIp.write(out);
        totalByte.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        amountOfIp.readFields(in);
        totalByte.readFields(in);
    }

    @Override
    public int compareTo(PairWritable pairWritable) {
        int compare = amountOfIp.compareTo(pairWritable.amountOfIp);
        if (compare != 0) {
            return compare;
        }
        return totalByte.compareTo(pairWritable.totalByte);
    }

    @Override
    public int hashCode() {
        return amountOfIp.hashCode() * 163 + totalByte.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PairWritable) {
            PairWritable pairWritable = (PairWritable) o;
            return amountOfIp.equals(pairWritable.amountOfIp) && totalByte.equals(pairWritable.totalByte);
        }
        return false;
    }

    @Override
    public String toString() {
        return amountOfIp.toString() + "\t" + totalByte.toString();
    }

    public static class PairComparator extends WritableComparator {

        private Logger logger = Logger.getLogger(PairComparator.class);
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public PairComparator() {
            super(PairWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int ipLength1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int ipLength2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int compare = TEXT_COMPARATOR.compare(b1, s1, ipLength1, b2, s2, ipLength2);
                if (compare != 0) {
                    return compare;
                }
                return TEXT_COMPARATOR.compare(b1, s1, l1 - ipLength1, b2, s2, l2 - ipLength2);

            } catch (IOException e) {
                logger.error("Exception while reading line in RawComparator" + Arrays.toString(b1) + Arrays.toString(b2) + e);
                throw new IllegalArgumentException();
            }
        }

        static {
            WritableComparator.define(PairWritable.class, new PairComparator());
        }
    }
}


