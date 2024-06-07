package com.jvcats.dataframe.base;

import java.util.Arrays;
import java.util.Objects;

/**
 * A column of data in a DataFrame.
 *
 * @author henry
 */
public class Column {
    private String header;
    private double[] dblObs;
    private String[] strObs;
    private int[] intObs;

    public Column() {

    }

    /**
     * Create a new column with a header and double values.
     *
     * @param name The header of the column.
     * @param dbls The double values of the column.
     */
    public Column(String name, double... dbls) {
        this.header = name;
        this.dblObs = dbls;
    }

    /**
     * Create a new column with a header and string values.
     *
     * @param name The header of the column.
     * @param strs The string values of the column.
     */
    public Column(String name, String... strs) {
        this.header = name;
        this.strObs = strs;
    }

    /**
     * Create a new column with a header and integer values.
     *
     * @param name The header of the column.
     * @param ints The integer values of the column.
     */
    public Column(String name, int... ints) {
        this.header = name;
        this.intObs = ints;
    }

    /**
     * Convert the column to a DataFrame.
     *
     * @return A new DataFrame with the column as the only column.
     */
    public DataFrame asDataFrame() {
        Column[] col = new Column[1];
        if (this.getDbls() != null) col[0] = new Column(this.header, this.getDbls());
        else if (this.strObs != null) {
            col[0] = new Column(this.header, this.strObs);
        } else if (this.intObs != null) {
            col[0] = new Column(this.header, this.intObs);
        }
        return (new DataFrame(col));
    }

    /**
     * Bind two columns together.
     *
     * @param col2 The second column to bind.
     * @return A new column with the two columns bound together.
     */
    public Column rbind(Column col2) {
        if (this.getDbls() != null) {
            double[] dbls = new double[this.nrow() + col2.nrow()];
            for (int i = 0; i < this.nrow(); i++) {
                dbls[i] = this.dblObs[i];
            }
            for (int i = this.nrow(); i < this.nrow() + col2.nrow(); i++) {
                dbls[i] = col2.dblObs[i - this.nrow()];
            }
            this.dblObs = dbls;
        } else if (this.getStrs() != null) {
            String[] strs = new String[this.nrow() + col2.nrow()];
            for (int i = 0; i < this.nrow(); i++) {
                strs[i] = this.strObs[i];
            }
            for (int i = this.nrow(); i < this.nrow() + col2.nrow(); i++) {
                strs[i] = col2.strObs[i - this.nrow()];
            }
            this.strObs = strs;
        } else if (this.getInts() != null) {
            int[] ints = new int[this.nrow() + col2.nrow()];
            for (int i = 0; i < this.nrow(); i++) {
                ints[i] = this.intObs[i];
            }
            for (int i = this.nrow(); i < this.nrow() + col2.nrow(); i++) {
                ints[i] = col2.intObs[i - this.nrow()];
            }
            this.intObs = ints;
        }
        return (this);
    }

    /**
     * Get the header of the column.
     *
     * @return The header of the column.
     */
    public String getHeader() {
        return (this.header);
    }

    /**
     * Set the header of the column.
     *
     * @param name The new header of the column.
     */
    public void setHeader(String name) {
        this.header = name;
    }

    /**
     * Get the double values of the column.
     *
     * @return The double values of the column.
     */
    public double[] getDbls() {
        return (this.dblObs);
    }

    /**
     * Set the double value at a specific index.
     *
     * @param dbl The new double value.
     * @param where The index to set the value at.
     */
    public void setDbl(double dbl, int where) {
        this.getDbls()[rPos(where)] = dbl;
    }

    /**
     * Set the double values of the column.
     *
     * @param dbls The new double values of the column.
     */
    public void setDblObs(double[] dbls) {
        this.dblObs = dbls;
    }

    /**
     * Get the string values of the column.
     *
     * @return The string values of the column.
     */
    public String[] getStrs() {
        return (this.strObs);
    }

    /**
     * Set the string value at a specific index.
     *
     * @param str The new string value.
     * @param where The index to set the value at.
     */
    public void setStr(String str, int where) {
        this.strObs[rPos(where)] = str;
    }

    /**
     * Set the string values of the column.
     *
     * @param strs The new string values of the column.
     */
    public void setStrObs(String[] strs) {
        this.strObs = strs;
    }

    /**
     * Get the integer values of the column.
     *
     * @return The integer values of the column.
     */
    public int[] getInts() {
        return (this.intObs);
    }

    /**
     * Set the integer value at a specific index.
     *
     * @param anInt The new integer value.
     * @param where The index to set the value at.
     */
    public void setInt(int anInt, int where) {
        this.intObs[rPos(where)] = anInt;
    }

    /**
     * Set the integer values of the column.
     *
     * @param ints The new integer values of the column.
     */
    public void setIntObs(int[] ints) {
        this.intObs = ints;
    }

    /**
     * Get the number of rows in the column.
     *
     * @return The number of rows in the column.
     */
    public int nrow() {
        int rowNum = 0;
        if (this.getDbls() != null) rowNum = this.getDbls().length;
        else if (this.getStrs() != null) rowNum = this.getStrs().length;
        else if (this.getInts() != null) rowNum = this.getInts().length;
        return rowNum;
    }

    /**
     * Print the column to the console.
     */
    public void show() {
        show(nrow());
    }

    /**
     * Print the first n rows of the column to the console.
     *
     * @param head The number of rows to print. If head is greater than the number of rows in the column, all rows will be printed.
     */
    public void show(int head) {
        int showSize = this.nrow();
        if (head < showSize) showSize = head;
        System.out.print("\u001B[36m" + "Names:" + "\u001B[0m");
        System.out.print("\u001B[34m" + " / " + this.header + "\u001B[0m");
        System.out.println("");
        System.out.println("\u001B[36m" + "Column: " + this.nrow() + "\u001B[0m");
        for (int i = 0; i < showSize; ++i) {
            {
                System.out.print("\u001B[36m" + (i + 1) + ":" + "\u001B[0m");
            }
            if (this.getDbls() != null) {
                System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getDbls()[i]);
            } else if (this.getStrs() != null) {
                System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getStrs()[i]);
            } else if (this.getInts() != null) {
                System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getInts()[i]);
            }
            System.out.println("");
        }
        if (showSize < this.nrow())
            System.out.println("\u001B[36m" + (this.nrow() - showSize) + " more row(s)..." + "\u001B[0m");
    }

    /**
     * Deep copy of the column.
     *
     * @param old The column to copy.
     * @return A new column with the same values as the old column.
     */
    public Column clone(Column old){
        this.header = old.header;
        if(old.dblObs!=null){
            this.dblObs = new double[old.dblObs.length];
            System.arraycopy(old.dblObs, 0, this.dblObs, 0, old.dblObs.length);
        }else if(old.strObs!=null){
            this.strObs = new String[old.strObs.length];
            for(int i=0; i<old.strObs.length; i++){
                this.strObs[i] = old.strObs[i];
            }
        }else if(old.intObs!=null){
            this.intObs = new int[old.intObs.length];
            for(int i=0; i<old.intObs.length; i++){
                this.intObs[i] = old.intObs[i];
            }
        }
        return this;
    }
    private int rPos(int i) {
        int ind = i < 0 ? this.nrow() + i : i;
        if(ind >= this.nrow()){
            throw new IndexOutOfBoundsException("Index: " + i + ", Row Size: " + this.nrow());
        }
        return (ind);
    }

    /**
     * Check if two columns are equal.
     *
     * @param o The other column to compare.
     * @return True if the two columns are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Objects.equals(header, column.header) && Objects.deepEquals(dblObs, column.dblObs) && Objects.deepEquals(strObs, column.strObs) && Objects.deepEquals(intObs, column.intObs);
    }

    /**
     * Get the hash code of the column.
     *
     * @return The hash code of the column.
     */
    @Override
    public int hashCode() {
        return Objects.hash(header, Arrays.hashCode(dblObs), Arrays.hashCode(strObs), Arrays.hashCode(intObs));
    }
}
