package com.jvcats.dataframe.base;

import java.util.Arrays;
import java.util.Objects;

public class OrderKey {
    private String name;
    private int[] values;

    OrderKey(){}

    OrderKey(String name, int[] values) {
        this.name = name;
        this.values = values;
    }

    String getName() {
        return name;
    }

    int[] getValues() {
        return values;
    }
    void setName(String name){
        this.name = name;
    }

    void setValues(int[] values){
        this.values = values;
    }

    void reverse(int nrow){
        int[] reversed = new int[nrow];
        if (this.values == null) {
            for (int i = 0; i < nrow; i++) {
                reversed[i] = i;
            }
            this.name = "reverse";
        }else{
            reversed = this.values.clone();
            this.name += "; reverse";
        }
        int temp = 0;
        for (int i = 0; i < reversed.length / 2; i++) {
            temp = reversed[i];
            reversed[i] = reversed[reversed.length - i - 1];
            reversed[reversed.length - i - 1] = temp;
        }
        this.values = reversed;
    }

    void movRow(int nrow, int which, int to){
        int from = which;
        boolean moveFront = true;
        int[] newOrder = new int[nrow];
        if (from<to) moveFront = false;
        if(moveFront){
            for(int i=0; i<nrow; i++){
                if(i>to && i<=from){
                    newOrder[i] = i - 1;
                }else if(i != to){
                    newOrder[i] = i;
                }
            }
        }else {
            for (int i = 0; i < nrow; i++) {
                if (i >= from && i < to) {
                    newOrder[i] = i + 1;
                } else if(i != to){
                    newOrder[i] = i;
                }
            }
        }
        newOrder[to] = from;
        this.setValues(newOrder);
        this.setName(this.name + "; move " + which + " to " + to);
    }

    public OrderKey copy(OrderKey old){
        if(old.name!=null){
            this.name = old.name;
        }
        if(old.values!=null){
            values = new int[old.values.length];
            for(int i=0;i<values.length;i++){
                this.values[i] = old.values[i];
            }
        }
        return this;
    }

    void clear(){
        this.name = null;
        this.values = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderKey orderKey = (OrderKey) o;
        return Objects.equals(name, orderKey.name) && Objects.deepEquals(values, orderKey.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, Arrays.hashCode(values));
    }
}
