/**
 * Created by liying on 2018/11/9.
 */

import java.io.Serializable;

import org.apache.spark.graphx.EdgeTriplet;

import scala.runtime.AbstractFunction1;

public class AbsFunc1 extends AbstractFunction1<EdgeTriplet<Item, Double>, Object> implements Serializable{

    @Override
    public Object apply(EdgeTriplet<Item, Double> v1) {
        return v1.attr()>0.90;
//        return !v1.dstAttr().getName() .equals( "other") && !v1.srcAttr().getName().equals("other");
    }
}
