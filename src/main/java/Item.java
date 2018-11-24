import lombok.Data;

import java.io.Serializable;

/**
 * @author deng
 * @date 2018/11/8
 */
@Data
public class Item implements Serializable {
    private long id;
    public String name;
    public String type;
    public int count;

    public Item(long id, String name, int count, String type) {
        this.id = id;
        this.count = count;
        this.name = name;
        this.type = type;
    }
}
