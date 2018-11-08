import java.io.Serializable;

/**
 * @author deng
 * @date 2018/11/8
 */
public class Item implements Serializable {
    public String name;
    public String type;

    public int count;

    public Item(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
