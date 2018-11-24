import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author deng
 * @date 2018/11/9
 */
public class GraphData {

    public static void main(String[] args) {
        new GraphData().calRate();
    }

    public void calRate() {
        String degrees = FileUtil.readString("all-nodes.txt");
        Map<Long, Integer> sumMap = new HashMap<>();
        for (String line : degrees.split("\n")) {
            sumMap.put(Long.parseLong(line.split(",")[0]), Integer.parseInt(line.split(",")[2]));
        }

        String links = FileUtil.readString("edges.txt");
        StringBuilder rateLinks = new StringBuilder();
        DecimalFormat df = new DecimalFormat("0.0000");
        for (String line : links.split("\n")) {
            String[] params = line.split(",");
            rateLinks.append(line.substring(0, line.lastIndexOf(",") + 1))
                    .append(df.format(1.0* Integer.parseInt(params[2]) / sumMap.get(Long.parseLong(params[0]))))
                    .append("\n");
        }
        FileUtil.writeString("rateLinks.txt", rateLinks.toString());
    }

}
