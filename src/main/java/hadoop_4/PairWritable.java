package hadoop_4;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PairWritable implements WritableComparable<PairWritable> {

    private Text city;
    private Text operatingSystem;

    public PairWritable() {
        set(new Text(), new Text());
    }

    public PairWritable(String city, String operatingSystem) {
        set(new Text(city), new Text(operatingSystem));
    }

    public PairWritable(Text city, Text operatingSystem) {
        set(city, operatingSystem);
    }

    public void set(Text city, Text operatingSystem) {
        this.city = city;
        this.operatingSystem = operatingSystem;
    }

    public Text getCity() {
        return city;
    }

    public Text  getOperatingSystem() {
        return operatingSystem;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        city.write(out);
        operatingSystem.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        city.readFields(in);
        operatingSystem.readFields(in);
    }

    @Override
    public int compareTo(PairWritable pairWritable) {
        int compare = city.compareTo(pairWritable.city);
        if (compare != 0) {
            return compare;
        }
        return operatingSystem.compareTo(pairWritable.operatingSystem);
    }

    @Override
    public int hashCode() {
        return city.hashCode() * 163 + operatingSystem.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PairWritable) {
            PairWritable pairWritable = (PairWritable) o;
            return city.equals(pairWritable.city) && operatingSystem.equals(pairWritable.operatingSystem);
        }
        return false;
    }

    @Override
    public String toString() {
        return city.toString() + "\t" + operatingSystem.toString();
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

