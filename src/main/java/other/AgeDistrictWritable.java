package other;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private int age;
    private Text district;

    public AgeDistrictWritable() {
        this.age = 0;
        this.district = new Text();
    }

    public AgeDistrictWritable(int age, String district) {
        this.age = age;
        this.district = new Text(district);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(age);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age = in.readInt();
        district.readFields(in);
    }

    // Getters and setters for age and district fields
    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Text getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = new Text(district);
    }

    @Override
    public String toString() {
        return age + "\t" + district.toString();
    }
}
