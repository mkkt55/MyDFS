import java.io.FileWriter;
import java.io.IOException;

public class DataGenerator {
    public static void main(String[] args) throws IOException {
        FileWriter f = new FileWriter(args[0]);
        for (long i =0 ;i < 99999999; i++) {
            f.write(i + "\n");
        }
        System.out.println("OK");
    }
}
