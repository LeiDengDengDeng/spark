/**
 * Created by liying on 2018/11/8.
 */
import java.io.*;

public class FileUtil {
    public static void writeString(String fileName, String content) {
        FileWriter fileWriter = null;
        try {
            File file = new File(fileName);
            if (!file.exists()) file.createNewFile();

            fileWriter = new FileWriter(file);
            fileWriter.write(content);
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String readString(String filePath) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(filePath)));

            byte[] b = new byte[bis.available()];
            bis.read(b);

            return new String(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String readString(File file) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));

            byte[] b = new byte[bis.available()];
            bis.read(b);

            return new String(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}