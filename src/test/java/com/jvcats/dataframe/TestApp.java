package com.jvcats.dataframe;

import com.jvcats.dataframe.base.*;
import org.junit.jupiter.api.*;

public class TestApp {
    @Test
    public void testDf(){
        Column names = new Column("Last-Name", "Amy", "Ben", "Cassie", "David");
        Column ages = new Column("Age",20, 25, 21, 24);
        Column heights = new Column("Height", 160., 170., 165., 180.);
        Column scores1 = new Column("Math", 40, 60, 80, 80);
        Column scores2 = new Column("English", 60, 60, 70, 90);
        DataFrame df0 = new DataFrame(names,ages,heights,scores1,scores2);
//        String file = "";
//        df0.writeTable(file, ",");
//        DataFrame df1 = new DataFrame();
//        try {
//            df1 = DataFrame.readTable(file, ",");
//        } catch (Exception e) {
//            System.out.println("Error: cannot read");
//        }
        DataFrame df1 = new DataFrame().clone(df0);
        DataFrame df1A = df1.addFilterGt("Age", 20).and()
                .addFilterColGt("Math", "English")
                .filter();
        df1.addCol(new Column("History", 100, 70, 50, 80), -1);
        df1.delCol("English");
        
        DataFrame df1B = df1.setOrderKey("Height")
                .setOKeyReverse()
                .order();
        
        Column ages2 = new Column("Age",23, 28, 24, 27);
        Column heights2 = new Column("Height", 165., 170., 170., 180.);
        Column scores12 = new Column("Math", 80, 30, 70, 80);
        Column scores22 = new Column("History", 90, 40, 70, 100);
        DataFrame df2 = new DataFrame(names, ages2, heights2, scores12, scores22);
        DataFrame df2A = df2.setFilterKeyFrom(df1, true)
                .filter();
        DataFrame df2B = df2.setOrderKeyFrom(df1).order();

        Assertions.assertEquals(df1A.getCol("Last-Name"), df2A.getCol(0));
        Assertions.assertEquals(df1B.getCol("Last-Name"), df2B.getCol(0));
        
    }
}
