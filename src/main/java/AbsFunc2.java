/**
 * Created by liying on 2018/11/9.
 */
import java.io.Serializable;

import scala.runtime.AbstractFunction2;

public class AbsFunc2 extends AbstractFunction2<Object, Item, Object> implements Serializable{

    @Override
    public Object apply(Object arg0, Item arg1) {

        return !arg1.getName().equals("other");
    }

}