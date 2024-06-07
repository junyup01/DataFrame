package com.jvcats.dataframe.base;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.ArrayList;

/**
 * The DataFrame class is the main class of the DataFrame library. It is used to store and manipulate data in a tabular format.
 * It has several methods for data manipulation, including filtering, sorting, grouping, and aggregation.
 *
 * @author henry
 */
public class DataFrame {
    private ArrayList<Column> columns;
    private FilterKey filterKey = new FilterKey();
    private boolean[] finalFilterValues;
    private GroupKey groupKey = new GroupKey();
    private OrderKey orderKey = new OrderKey();
    private String andOrOrNow = "first";

    public DataFrame() {}

    /**
     * Create a DataFrame by given Columns.
     *
     * @param cols Columns used for creating the DataFrame.
     */
    public DataFrame(Column... cols) {
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Create a DataFrame by double 2D array. The number of columns is the number of outer layer of the array.
     * e.g. [3][5] is for a 5 by 3 DataFrame.
     *
     * @param colnames The names of the columns.
     * @param dbls The double values of the DataFrame.
     */
    public DataFrame(String[] colnames, double[][] dbls) {
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], dbls[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }
    /**
     * Create a DataFrame by string 2D array. The number of columns is the number of outer layer of the array.
     * e.g. [3][5] is for a 5 by 3 DataFrame.
     *
     * @param colnames The names of the columns.
     * @param strs The string values of the DataFrame.
     */
    public DataFrame(String[] colnames, String[][] strs) {
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], strs[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }
    /**
     * Create a DataFrame by int 2D array. The number of columns is the number of outer layer of the array.
     * e.g. [3][5] is for a 5 by 3 DataFrame.
     *
     * @param colnames The names of the columns.
     * @param ints The int values of the DataFrame.
     */
    public DataFrame(String[] colnames, int[][] ints) {
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], ints[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Create a DataFrame by given double array.
     *
     * @param dbls The double values of the DataFrame.
     */
    public DataFrame(double[][] dbls) {
        String[] colnames = new String[dbls.length];
        for (int i = 1; i < dbls.length + 1; i++) {
            colnames[i - 1] = "Col" + i;
        }
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], dbls[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Create a DataFrame by given string array.
     *
     * @param strs The string values of the DataFrame.
     */
    public DataFrame(String[][] strs) {
        String[] colnames = new String[strs.length];
        for (int i = 1; i < strs.length + 1; i++) {
            colnames[i - 1] = "Col" + i;
        }
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], strs[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Create a DataFrame by given int array.
     *
     * @param ints The int values of the DataFrame.
     */
    public DataFrame(int[][] ints) {
        String[] colnames = new String[ints.length];
        for (int i = 1; i < ints.length + 1; i++) {
            colnames[i - 1] = "Col" + i;
        }
        Column[] cols = new Column[colnames.length];
        for (int j = 0; j < colnames.length; ++j) {
            Column col = new Column(colnames[j], ints[j]);
            cols[j] = col;
        }
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Check the object's data types of all columns.
     *
     * @return A DataFrame with the object's data types of all columns.
     */
    public DataFrame colTypesDF() {
        String[][] colsType = new String[2][this.ncol()];
        for (int i = 0; i < this.ncol(); i++) {
            if (this.getCol(i).getDbls() != null) {
                colsType[1][i] = "dbl";
            } else if (this.getCol(i).getStrs() != null) {
                colsType[1][i] = "str";
            } else if (this.getCol(i).getInts() != null) {
                colsType[1][i] = "int";
            }
        }
        colsType[0] = this.getAllColnames();
        return (new DataFrame(new String[]{"Names", "Types"}, colsType));
    }

    /**
     * Check the column's data type.
     *
     * @param which The number of the column.
     * @return The data type of the column.
     */
    public String colType(int which) {
        String colType = "";
        int whichOne = this.cPos(which);
        if (this.getCol(whichOne).getDbls() != null) {
            colType = "dbl";
        } else if (this.getCol(whichOne).getStrs() != null) {
            colType = "str";
        } else if (this.getCol(whichOne).getInts() != null) {
            colType = "int";
        }
        return (colType);
    }

    /**
     * Check the column's data type.
     *
     * @param colname The column name of the column.
     * @return A string of "dbl", "str", or "int".
     */
    public String colType(String colname) {
        int which = this.indexCol(colname);
        return (this.colType(which));
    }

    /**
     * Check the object's keys status.
     *
     * @return A DataFrame with the keys' status.
     */
    public DataFrame keysStatusDF() {
        int size = this.filterKey.getValues() == null ? 0 : this.filterKey.getValues().size();
        String[][] ifExist = new String[2][size+2];
        if(this.filterKey.getValues() != null){
            for(int i = 0; i < this.filterKey.getValues().size(); i++){
                ifExist[1][i] = this.filterKey.getNames().get(i);
                ifExist[0][i] = "FilterKey" + (i+1);
            }
        }
        ifExist[1][size] = this.orderKey == null ? "null" : this.orderKey.getName();
        ifExist[1][size+1] = this.groupKey == null ? "null" : this.groupKey.getName();
        ifExist[0][size] = "OrderKey";
        ifExist[0][size+1] = "GroupKey";
        return (new DataFrame(new String[]{"KeyName", "Status"}, ifExist));
    }

    /**
     * Select the specific column's observation from a DataFrame.
     *
     * @param row The row of the observation.
     * @param col The column of the observation.
     * @return The column of the observation in specified location.
     */
    public Column locCol(int row, int col) {
        return (this.rloc(row).getCol(col));
    }

    /**
     * Select the specific column's observation from a DataFrame.
     *
     * @param row The row of the observation.
     * @param colname The column name of the observation.
     * @return The column of the observation in specified location.
     */
    public Column locCol(int row, String colname) {
        return (this.rloc(row).getCol(colname));
    }

    /**
     * Select a double value from a double column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @return The value in specified location.
     */
    public double locDbl(int row, int col) {
        return (this.locCol(row, col).getDbls()[0]);
    }

    /**
     * Select a string value from a string column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @return The value in specified location.
     */
    public String locStr(int row, int col) {
        return (this.locCol(row, col).getStrs()[0]);
    }

    /**
     * Select an integer value from an integer column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @return The value in specified location.
     */
    public int locInt(int row, int col) {
        return (this.locCol(row, col).getInts()[0]);
    }

    /**
     * Set a double value to a double column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @param value The value to be set.
     */
    public void setDbl(int row, int col, double value) {
        this.locCol(row, col).setDbl(value, 0);
    }

    /**
     * Set a string value to a string column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @param value The value to be set.
     */
    public void setStr(int row, int col, String value) {
        this.locCol(row, col).setStr(value, 0);
    }

    /**
     * Set an integer value to an integer column of a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @param value The value to be set.
     */
    public void setInt(int row, int col, int value) {
        this.locCol(row, col).setInt(value, 0);
    }

    /**
     * Select a column from a DataFrame.
     *
     * @param colname The name of the column.
     * @return The column in specified location.
     */
    public Column getCol(String colname) {
        return (this.getCol(this.indexCol(colname)));
    }

    /**
     * Select a column from a DataFrame.
     *
     * @param which The number of the column.
     * @return The column in specified location.
     */
    public Column getCol(int which) {
        return (this.columns.get(this.cPos(which)));
    }


    /**
     * Get a value DataFrame from a DataFrame.
     *
     * @param row The row of the value.
     * @param col The column of the value.
     * @return The double value DataFrame.
     */
    public DataFrame loc(int row, int col) {
        return (this.rloc(row).cloc(col));
    }

    /**
     * Get a range of values DataFrame from a DataFrame.
     *
     * @param row1 The row from.
     * @param row2 The row to, not inclusive.
     * @param col1 The column from.
     * @param col2 The column to, not inclusive.
     * @return The range of values DataFrame.
     */
    public DataFrame loc(int row1, int row2, int col1, int col2) {
        return (this.rloc(row1, row2).cloc(col1, col2));
    }

    /**
     * Get a range of values DataFrame from a DataFrame.
     *
     * @param row1 The row from.
     * @param row2 The row to, not inclusive.
     * @param colname1 The column name from.
     * @param colname2 The column name to, not inclusive.
     * @return The range of values DataFrame.
     */
    public DataFrame loc(int row1, int row2, String colname1, String colname2) {
        return (this.rloc(row1, row2).cloc(colname1, colname2));
    }

    /**
     * Select a column DataFrame from a DataFrame.
     *
     * @param colname The name of the column.
     * @return The column DataFrame in specified location.
     */
    public DataFrame cloc(String colname) {
        return (this.getCol(this.indexCol(colname)).asDataFrame());
    }

    /**
     * Select a column DataFrame from a DataFrame.
     *
     * @param which The number of the column.
     * @return The column DataFrame in specified location.
     */
    public DataFrame cloc(int which) {
        which = this.cPos(which);
        return (this.getCol(which).asDataFrame());
    }

    /**
     * Select columns from a DataFrame.
     *
     * @param colname The names of the columns.
     * @return The columns in specified location.
     */
    public DataFrame cloc(String[] colname) {
        Column[] cols = new Column[colname.length];
        for (int i = 0; i < colname.length; ++i) {
            cols[i] = this.getCol(colname[i]);
        }
        return (new DataFrame(cols));
    }

    /**
     * Select columns from a DataFrame.
     *
     * @param which The numbers of the columns.
     * @return The columns in specified location.
     */
    public DataFrame cloc(int[] which) {
        Column[] cols = new Column[which.length];
        for (int i = 0; i < which.length; ++i) {
            cols[i] = this.getCol(which[i]);
        }
        return (new DataFrame(cols));
    }

    /**
     * Select columns from a DataFrame.
     *
     * @param from The column from.
     * @param to   The column to, not inclusive.
     * @return The columns in specified location.
     */
    public DataFrame cloc(int from, int to) {
        Column[] cols = new Column[to - from];
        for (int i = from; i < to; ++i) {
            cols[i - from] = this.getCol(i);
        }
        return (new DataFrame(cols));
    }

    /**
     * Select columns from a DataFrame.
     *
     * @param from The column from.
     * @param to   The column to, not inclusive.
     * @return The columns in specified location.
     */
    public DataFrame cloc(String from, String to) {
        int from_which = this.indexCol(from);
        int to_which = this.indexCol(to);
        return (this.cloc(from_which, to_which));
    }

    /**
     * Select columns DataFrame from a DataFrame.
     *
     * @param logic true for selecting.
     * @return The columns DataFrame in specified location.
     */
    public DataFrame cloc(boolean[] logic) {
        int check = 0;
        for (boolean logic_i : logic) {
            check = logic_i ? check + 1 : check;
        }
        int[] selected = new int[check];
        int k = 0;
        for (int i = 0; i < this.ncol(); i++) {
            if (logic[i]) {
                selected[k] = i;
                k++;
            }
        }
        return (this.cloc(selected));
    }

    /**
     * Get double values from a double column of a DataFrame.
     *
     * @param colname The name of the column.
     * @return The double values in specified column.
     */
    public double[] getColDbls(String colname) {
        return (this.getCol(this.indexCol(colname)).getDbls());
    }

    /**
     * Get double values from a double column of a DataFrame.
     *
     * @param which The number of the column.
     * @return The double values in specified column.
     */
    public double[] getColDbls(int which) {
        return (this.getCol(this.rPos(which)).getDbls());
    }

    /**
     * Get string values from a string column of a DataFrame.
     *
     * @param colname The name of the column.
     * @return The string values in specified column.
     */
    public String[] getColStrs(String colname) {
        return (this.getCol(this.indexCol(colname)).getStrs());
    }

    /**
     * Get string values from a string column of a DataFrame.
     *
     * @param which The number of the column.
     * @return The string values in specified column.
     */
    public String[] getColStrs(int which) {
        return (this.getCol(this.rPos(which)).getStrs());
    }

    /**
     * Get integer values from a integer column of a DataFrame.
     *
     * @param colname The name of the column.
     * @return The integer values in specified column.
     */
    public int[] getColInts(String colname) {
        return (this.getCol(this.indexCol(colname)).getInts());
    }

    /**
     * Get integer values from a integer column of a DataFrame.
     *
     * @param which The number of the column.
     * @return The integer values in specified column.
     */
    public int[] getColInts(int which) {
        return (this.getCol(this.rPos(which)).getInts());
    }

    /**
     * Get the double values from a DataFrame which only contains double values.
     * @return The double values.
     */
    public double[][] asDblArray() {
        double[][] arr = new double[this.ncol()][this.nrow()];
        for (int j = 0; j < this.ncol(); j++) {
            arr[j] = this.getColDbls(j);
        }
        return (arr);
    }

    /**
     * Get the string values from a DataFrame which only contains string values.
     *
     * @return The string values.
     */
    public String[][] asStrArray() {
        String[][] arr = new String[this.ncol()][this.nrow()];
        for (int j = 0; j < this.ncol(); j++) {
            arr[j] = this.getColStrs(j);
        }
        return (arr);
    }

    /**
     * Get the integer values from a DataFrame which only contains integer values.
     *
     * @return The integer values.
     */
    public int[][] asIntArray() {
        int[][] arr = new int[this.ncol()][this.nrow()];
        for (int j = 0; j < this.ncol(); j++) {
            arr[j] = this.getColInts(j);
        }
        return (arr);
    }

    /**
     * Get all column names of a DataFrame.
     *
     * @return The column names.
     */
    public String[] getAllColnames() {
        String[] colNames = new String[this.ncol()];
        for (int i = 0; i < this.ncol(); i++) {
            colNames[i] = this.getCol(i).getHeader();
        }
        return (colNames);
    }

    /**
     * Get the total number of columns of a DataFrame.
     *
     * @return The total number.
     */
    public int ncol() {
        return (this.columns.size());
    }

    /**
     * Get the total number of rows of a DataFrame.
     *
     * @return The total number.
     */
    public int nrow() {
        int rowNum = 0;
        if (this.getCol(0).getDbls() != null) {
            rowNum = this.getCol(0).getDbls().length;
        } else if (this.getCol(0).getStrs() != null) {
            rowNum = this.getCol(0).getStrs().length;
        } else if (this.getCol(0).getInts() != null) {
            rowNum = this.getCol(0).getInts().length;
        }
        return (rowNum);
    }

    /**
     * Get the index number based on given column name.
     *
     * @param colname The name of the column.
     * @return The index of the column.
     */
    public int indexCol(String colname) {
        int which = -1;
        for (int i = 0; i < this.ncol(); i++) {
            if (colname.equals(this.getCol(i).getHeader())) {
                which = i;
                break;
            }
        }
        if(which == -1) {
            System.out.println("Error: Column "+colname+" not found");
            System.exit(0);
        }
        return (which);
    }

    /**
     * Get the index numbers based on given column names.
     *
     * @param colname The names of the columns.
     * @return The indices of the columns.
     */
    public int[] indexCol(String[] colname) {
        int[] which = new int[colname.length];
        for(int i=0; i<colname.length; i++){
            which[i] = this.indexCol(colname[i]);
        }
        return (which);
    }

    /**
     * Get the column name based on given index number.
     *
     * @param which The index of the column.
     * @return The column name of the column.
     */
    public String getColname(int which) {
        return (this.getCol(which).getHeader());
    }

    /**
     * Get the column names based on given index numbers.
     *
     * @param which The indices of the columns.
     * @return The column names of the columns.
     */
    public String[] getColname(int[] which) {
        String[] headers = new String[which.length];
        for (int i = 0; i < which.length; ++i) {
            which[i] = this.cPos(which[i]);
        }
        for (int i = 0; i < which.length; ++i) {
            headers[i] = this.getCol(which[i]).getHeader();
        }
        return (headers);
    }

    /**
     * Get the column type based on given index number.
     *
     * @param from The index of the column.
     * @param to The index of the column.
     * @return The column names of the columns.
     */
    public String[] getColname(int from, int to) {
        from = this.cPos(from);
        to = this.cPos(to);
        String[] headers = new String[from - to];
        int[] which = new int[from - to];
        for (int i = from; i < to; ++i) {
            which[i - from] = i;
        }
        headers = this.getColname(which);
        return (headers);
    }

    /**
     * Get the column name next to the given column name.
     *
     * @param thisColname The name of the column before the next one.
     * @return The column name of the next column.
     */
    public String getNextColname(String thisColname) {
        return (this.getCol(this.indexCol(thisColname) + 1).getHeader());
    }

    /**
     * Order and then filter the original DataFrame based on the Keys.
     *
     * @return The DataFrame.
     */
    public DataFrame exeKeysOrg() {
        if (this.orderKey != null) {
            orderOrg(this);
        }
        if (this.finalFilterValues != null) {
            filterOrg(this);
        }
        return (this);
    }

    private DataFrame exeKeys() {
        DataFrame df = new DataFrame();
        df.clone(this);
        if (df.orderKey != null) {
            df = df.order();
        }
        if (df.finalFilterValues != null) {
            df = df.filter();
        }
        return (df);
    }

    /**
     * Filter the DataFrame based on the FilterKey.
     *
     * @return The filtered DataFrame.
     */
    public DataFrame filter() {
        if (this.finalFilterValues == null) {
            System.out.println("Error: Set the filterKey first");
            System.exit(0);
        }
        boolean[] logic = this.finalFilterValues;
        Column[] cols = new Column[this.ncol()];
        int k = 0;
        for (int j = 0; j < this.ncol(); ++j) {
            k = 0;
            if (this.colType(j).equals("dbl")) {
                cols[j] = new Column(this.getColname(j), new double[this.nrow()]);
                for (int i = 0; i < this.nrow(); ++i) {
                    if (logic[i]) {
                        cols[j].setDbl(this.getCol(j).getDbls()[i], k);
                        k++;
                    }
                }
            } else if (this.colType(j).equals("str")) {
                cols[j] = new Column(this.getColname(j), new String[this.nrow()]);
                for (int i = 0; i < this.nrow(); ++i) {
                    if (logic[i]) {
                        cols[j].setStr(this.getCol(j).getStrs()[i], k);
                        k++;
                    }
                }
            } else if (this.colType(j).equals("int")) {
                cols[j] = new Column(this.getColname(j), new int[this.nrow()]);
                for (int i = 0; i < this.nrow(); ++i) {
                    if (logic[i]) {
                        cols[j].setInt(this.getCol(j).getInts()[i], k);
                        k++;
                    }
                }
            }
            if (k == 0) {
                System.out.println("Error: No observations meet the request");
                System.exit(0);
            }
        }
        DataFrame df = new DataFrame();
        df.columns = new ArrayList<>();
        df.columns.addAll(Arrays.asList(cols));
        return (df.rloc(0, k).statusClone(this));
    }

    private static void filterOrg(DataFrame df) {
        if (df.finalFilterValues == null) {
            System.out.println("Error: Set the filterKey first");
            System.exit(0);
        }
        boolean[] logic = df.finalFilterValues;
        int k = 0;
        for (int j = 0; j < df.ncol(); ++j) {
            k = 0;
            if (df.colType(j).equals("dbl")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    if (logic[i]) {
                        df.getCol(j).setDbl(df.getCol(j).getDbls()[i], k);
                        k++;
                    }
                }
            } else if (df.colType(j).equals("str")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    if (logic[i]) {
                        df.getCol(j).setStr(df.getCol(j).getStrs()[i], k);
                        k++;
                    }
                }
            } else if (df.colType(j).equals("int")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    if (logic[i]) {
                        df.getCol(j).setInt(df.getCol(j).getInts()[i], k);
                        k++;
                    }
                }
            }
//            df = df.delFilterKey();
            if (k == 0) {
                System.out.println("Error: No observations meet the request");
                System.exit(0);
            }
        }
        rlocOrg(df, 0, k);
        df.delFilterKey();
    }

    /**
     * Delete all keys from the DataFrame.
     *
     * @return The DataFrame without any keys.
     */
    public DataFrame delAllKeys() {
        this.delFilterKey().delOrderKey().delGroupKey();
        return (this);
    }

    /**
     * Delete FilterKey from the DataFrame.
     *
     * @return The DataFrame without FilterKey.
     */
    public DataFrame delFilterKey() {
        this.filterKey.clear();
        this.finalFilterValues = null;
        return (this);
    }
    protected void setFinalResult(boolean[] finalResult) {
        this.finalFilterValues = finalResult;
    }

    /**
     * Set the final filter values of the DataFrame.
     *
     * @param finalValues The final filter values.
     */
    public void setFinalFValues(boolean[] finalValues) {
        this.finalFilterValues = finalValues;
        if(!this.isFinalEqualToFilter())
            System.out.println("Warning: finalFilterKey is no longer consistent with the result of calcFinalFromFirst() method.");
    }

    /**
     * Get the final filter values of the DataFrame.
     *
     * @return The final filter values.
     */
    public boolean[] getFinalFValues() {
        return (this.finalFilterValues);
    }

    /**
     * Calculate the final filter values based on the current filter values and the new filter values.
     *
     * @param finalTemp The current final filter values.
     * @param newBool The new filter values.
     * @param andOrOr The logical operator to combine the new filter values with the current final filter values.
     * @return The final filter values.
     */
    public boolean[] calcFinalFValues(boolean[] finalTemp, boolean[] newBool, String andOrOr){
        boolean[] result = new boolean[this.filterKey.getValues().getFirst().length];
        for (int i = 0; i < newBool.length; i++) {
            if(andOrOr.equals("and")) {
                result[i] = newBool[i] && finalTemp[i];
            }else if(andOrOr.equals("or")) {
                result[i] = newBool[i] || finalTemp[i];
            }
        }
        return result;
    }

    /**
     * Update the final filter values based on the current filter values and the new filter values.
     */
    public void updateFinalFromLast(){
        if(this.finalFilterValues == null){
            this.finalFilterValues = this.filterKey.getValues().getFirst().clone();
        } else {
            this.finalFilterValues = this.calcFinalFValues(this.finalFilterValues, this.filterKey.getValues().getLast(), this.filterKey.getAndOrOr().getLast());
        }
    }

    /**
     * calculate the final filter values from the first filter values to the last.
     *
     * @return The final filter values.
     */
    public boolean[] calcFinalFromFirst(){
        boolean[] trueFinal = this.filterKey.getValues().getFirst().clone();
        for(int i=1; i<this.filterKey.getValues().size(); i++){
            trueFinal = this.calcFinalFValues(trueFinal,this.filterKey.getValues().get(i),this.filterKey.getAndOrOr().get(i));
        }
        return trueFinal;
    }

    /**
     * Check if the final filter values is equal to the result of calcFinalFromFirst() method.
     *
     * @return True if the final filter values is equal to the result of calcFinalFromFirst() method.
     */
    public boolean isFinalEqualToFilter(){
        return Arrays.equals(this.finalFilterValues, this.calcFinalFromFirst());
    }

    /**
     * Reset the final filter values to the result of calcFinalFromFirst() method.
     */
    public void resetFinalFromFirst(){
        this.finalFilterValues = this.calcFinalFromFirst();
    }

    /**
     * Set the FilterKey of the DataFrame from another DataFrame.
     *
     * @param df A DataFrame from which the FilterKey would be given.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with FilterKey set from the given DataFrame.
     */
    public DataFrame setFilterKeyFrom(DataFrame df, boolean updateFinalFValues) {
        this.filterKey.copy(df.filterKey);
        if (updateFinalFValues) this.resetFinalFromFirst();
        return (this);
    }

    /**
     * Add boolean filter values to the FilterKey.
     *
     * @param value The boolean filter values.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with boolean filter values added to the FilterKey.
     */
    public DataFrame addFilterValues(boolean[] value, boolean updateFinalFValues) {
        this.filterKey.addCriterion(value, this.andOrOrNow);
        if (updateFinalFValues) this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the chosen row indices as the filter values to the FilterKey.
     *
     * @param valueForTrues The chosen row indices.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with the chosen row indices as the filter values to the FilterKey.
     */
    public DataFrame addFilterTrue(int[] valueForTrues, boolean updateFinalFValues) {
        this.filterKey.addCriterion(valueForTrues, this.nrow(),this.andOrOrNow);
        if (updateFinalFValues) this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the chosen row indices as the filter values to the FilterKey.
     *
     * @param valueForTruesFrom The chosen row indices from.
     * @param valueForTruesTo The chosen row indices to.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with the chosen row indices as the filter values to the FilterKey.
     */
    public DataFrame addFilterTrue(int valueForTruesFrom, int valueForTruesTo, boolean updateFinalFValues) {
        this.filterKey.addCriterion(valueForTruesFrom, valueForTruesTo,this.nrow(),this.andOrOrNow);
        if (updateFinalFValues) this.updateFinalFromLast();
        return (this);
    }

    /**
     * Exclude the chosen row indices as the filter values to the FilterKey.
     *
     * @param keyForFalses The chosen row indices.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with the chosen row indices as the filter values to the FilterKey.
     */
    public DataFrame addFilterFalse(int[] keyForFalses, boolean updateFinalFValues){
        this.filterKey.addValuesByFalse(keyForFalses, this.nrow(),this.andOrOrNow);
        if (updateFinalFValues) this.updateFinalFromLast();
        return (this);
    }

    /**
     * Exclude the chosen row indices as the filter values to the FilterKey.
     *
     * @param valueForFalsesFrom The chosen row indices from.
     * @param valueForFalsesTo The chosen row indices to.
     * @param updateFinalFValues True if the final filter values should be updated.
     * @return The DataFrame with the chosen row indices as the filter values to the FilterKey.
     */
    public DataFrame addFilterFalse(int valueForFalsesFrom, int valueForFalsesTo, boolean updateFinalFValues) {
        this.filterKey.addValuesByFalse(valueForFalsesFrom, valueForFalsesTo,this.nrow(),this.andOrOrNow);
        if (updateFinalFValues) this.updateFinalFromLast();
        return (this);
    }

    /**
     * Set the logical operator "and" for the next filter values to be added to the FilterKey.
     *
     * @return The DataFrame after setting.
     */
    public DataFrame and(){
        this.andOrOrNow = "and";
        return this;
    }

    /**
     * Set the logical operator "or" for the next filter values to be added to the FilterKey.
     *
     * @return The DataFrame after setting.
     */
    public DataFrame or(){
        this.andOrOrNow = "or";
        return this;
    }

    /**
     * Add the criteria to the FilterKey by parsing the input string.
     *
     * @param criteriaToBeParsed The input string.
     * @return The DataFrame with the criteria added to the FilterKey.
     */
    public DataFrame addFilterByParsing(String criteriaToBeParsed){
        String[] criteria = criteriaToBeParsed.split(" ");
        if(criteria.length!= 4){
            System.out.println("Error: Criteria should be in the format of 'first/and/or column operator value'");
            System.exit(0);
        }
        this.andOrOrNow = criteria[0];
        String colname = criteria[1];
        String operator = criteria[2];
        String value = criteria[3];
        if(operator.equals(">")) {
            if (this.colType(colname).equals("dbl")) {
                this.addFilterGt(colname, Double.parseDouble(value));
            } else if (this.colType(colname).equals("int")) {
                this.addFilterGt(colname, Integer.parseInt(value));
            } else {
                System.out.println("Error: Column type should be either 'dbl' or 'int'");
                System.exit(0);
            }
        }else if(operator.equals("<")) {
            if (this.colType(colname).equals("dbl")) {
                this.addFilterLt(colname, Double.parseDouble(value));
            } else if (this.colType(colname).equals("int")) {
                this.addFilterLt(colname, Integer.parseInt(value));
            } else {
                System.out.println("Error: Column type should be either 'dbl' or 'int'");
                System.exit(0);
            }
        }else if(operator.equals(">=")) {
            if (this.colType(colname).equals("int")) {
                this.addFilterNlt(colname, Integer.parseInt(value));
            } else {
                System.out.println("Error: Column type should be 'int'");
                System.exit(0);
            }
        }else if(operator.equals("<=")) {
            if (this.colType(colname).equals("int")) {
                this.addFilterNgt(colname, Integer.parseInt(value));
            } else {
                System.out.println("Error: Column type should be 'int'");
                System.exit(0);
            }
        }else if(operator.equals("==")) {
            if (this.colType(colname).equals("int")) {
                this.addFilterEq(colname, Integer.parseInt(value));
            } else if (this.colType(colname).equals("str")) {
                this.addFilterEq(colname, value);
            } else {
                System.out.println("Error: Column type should be either 'int' or 'str'");
                System.exit(0);
            }
        }else if(operator.equals("!=")) {
            if (this.colType(colname).equals("int")) {
                this.addFilterNeq(colname, Integer.parseInt(value));
            } else if (this.colType(colname).equals("str")) {
                this.addFilterNeq(colname, value);
            } else {
                System.out.println("Error: Column type should be either 'int' or 'str'");
                System.exit(0);
            }
        }else{
            System.out.println("Error: Operator should be either '>' or '<' or '>=' or '<=' or '==' or '!='");
            System.exit(0);
        }
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is greater than given double value.
     *
     * @param colname The column name.
     * @param base The double value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterGt(String colname, double base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getDbls()[i] > base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, ">",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is less than given double value.
     *
     * @param colname The column name.
     * @param base The double value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterLt(String colname, double base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getDbls()[i] < base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "<",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is in the range of given double values.
     *
     * @param colname The column name.
     * @param base1 The double value for the lower bound.
     * @param base2 The double value for the upper bound, not inclusive.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterRange(String colname, double base1, double base2) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getDbls()[i] >= base1 && this.getCol(which_col).getDbls()[i] < base2) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, ">=",String.valueOf(base1), this.andOrOrNow);
        this.filterKey.addCriterion(logic, colname, "<=",String.valueOf(base2), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is greater than given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterGt(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] > base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, ">",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not less than given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNlt(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] >= base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, ">=",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is less than given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterLt(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] < base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "<",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not greater than given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNgt(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] <= base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "<=",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is in the range of given integer values.
     *
     * @param colname The column name.
     * @param base1 The integer value for the lower bound.
     * @param base2 The integer value for the upper bound, not inclusive.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterRange(String colname, int base1, int base2) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] >= base1 && this.getCol(which_col).getInts()[i] < base2) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, ">=",String.valueOf(base1), this.andOrOrNow);
        this.filterKey.addCriterion(logic, colname, "<=",String.valueOf(base2), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * A simple method to select rows of a DataFrame if the value in a column is equal to given integer value.
     * The FilterKey would only be set/changed automatically when using this method.
     *
     * @param colname
     * @param base
     * @return A DataFrame filtered by the method.
     */
    private DataFrame eq(String colname, int base) {
        DataFrame df = new DataFrame().clone(this).delAllKeys().addFilterEq(colname, base);
        return (df.filter().delAllKeys());
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is equal to given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterEq(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] == base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "==",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is contained in given integer values.
     *
     * @param colname The column name.
     * @param base The integer values.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterEq(String colname, int[] base){
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (Arrays.asList(base).contains(this.getCol(which_col).getInts()[i])) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "==",Arrays.toString(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not equal to given integer value.
     *
     * @param colname The column name.
     * @param base The integer value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNeq(String colname, int base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getInts()[i] != base) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "!=",String.valueOf(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not contained in given integer values.
     *
     * @param colname The column name.
     * @param base The integer values.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNeq(String colname, int[] base){
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (!Arrays.asList(base).contains(this.getCol(which_col).getInts()[i])) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "!=",Arrays.toString(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * A simple method to select rows of a DataFrame if the value in a column is the same as given string value.
     * The FilterKey would only be set/changed automatically when using this method.
     *
     * @param colname The column name.
     * @param base The string value.
     * @return A DataFrame filtered by the method.
     */
    private DataFrame eq(String colname, String base) {
        DataFrame df = new DataFrame().clone(this).delAllKeys().addFilterEq(colname, base);
        return (df.filter().delAllKeys());
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is the same as given string value.
     *
     * @param colname The column name.
     * @param base The string value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterEq(String colname, String base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (this.getCol(which_col).getStrs()[i].equals(base)) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "==",base, this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is contained in given string values.
     *
     * @param colname The column name.
     * @param base The string values.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterEq(String colname, String[] base){
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (Arrays.asList(base).contains(this.getCol(which_col).getStrs()[i])) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "==",Arrays.toString(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not the same as given string value.
     *
     * @param colname The column name.
     * @param base The string value.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNeq(String colname, String base) {
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (!this.getCol(which_col).getStrs()[i].equals(base)) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "!=",base, this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not contained in given string values.
     *
     * @param colname The column name.
     * @param base The string values.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterNeq(String colname, String[] base){
        boolean[] logic = new boolean[this.nrow()];
        int which_col = this.indexCol(colname);
        int check = 0;
        for (int i = 0; i < this.nrow(); ++i) {
            if (!Arrays.asList(base).contains(this.getCol(which_col).getStrs()[i])) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname, "!=",Arrays.toString(base), this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is greater than the value in another column.
     *
     * @param colname1 The column name for the first column.
     * @param colname2 The column name for the second column.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterColGt(String colname1, String colname2){
        boolean[] logic = new boolean[this.nrow()];
        int which_col1 = this.indexCol(colname1);
        int which_col2 = this.indexCol(colname2);
        int check = 0;
        double[] intsToDbls1 = new double[nrow()];
        double[] intsToDbls2 = new double[nrow()];
        if(this.colType(which_col1).equals("int")){
            for(int i=0; i<nrow(); i++) {
                intsToDbls1[i] = this.getCol(which_col1).getInts()[i];
            }
        }else if(this.colType(which_col1).equals("dbl")){
            intsToDbls1 = this.getCol(which_col1).getDbls();
        }
        if(this.colType(which_col2).equals("int")){
            for(int i=0; i<nrow(); i++) {
                intsToDbls2[i] = this.getCol(which_col2).getInts()[i];
            }
        }else if(this.colType(which_col2).equals("dbl")){
            intsToDbls2 = this.getCol(which_col2).getDbls();
        }
        for (int i = 0; i < this.nrow(); ++i) {
            if (intsToDbls1[i] > intsToDbls2[i]) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname1, ">",colname2, this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is not less than the value in another column.
     *
     * @param colname1 The column name for the first column.
     * @param colname2 The column name for the second column.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterColNlt(String colname1, String colname2){
        boolean[] logic = new boolean[this.nrow()];
        int which_col1 = this.indexCol(colname1);
        int which_col2 = this.indexCol(colname2);
        int check = 0;
        double[] intsToDbls1 = new double[nrow()];
        double[] intsToDbls2 = new double[nrow()];
        if(this.colType(which_col1).equals("int")){
            for(int i=0; i<nrow(); i++) {
                intsToDbls1[i] = this.getCol(which_col1).getInts()[i];
            }
        }else if(this.colType(which_col1).equals("dbl")){
            intsToDbls1 = this.getCol(which_col1).getDbls();
        }
        if(this.colType(which_col2).equals("int")){
            for(int i=0; i<nrow(); i++) {
                intsToDbls2[i] = this.getCol(which_col2).getInts()[i];
            }
        }else if(this.colType(which_col2).equals("dbl")){
            intsToDbls2 = this.getCol(which_col2).getDbls();
        }
        for (int i = 0; i < this.nrow(); ++i) {
            if (intsToDbls1[i] >= intsToDbls2[i]) {
                logic[i] = true;
                check++;
            } else {
                logic[i] = false;
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname1, ">=",colname2, this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }

    /**
     * Add the filter values by filter the DataFrame if the value in a column is equal to the value in another column.
     *
     * @param colname1 The column name for the first column.
     * @param colname2 The column name for the second column.
     * @return The DataFrame with the filter values added to the FilterKey.
     */
    public DataFrame addFilterColEq(String colname1, String colname2){
        boolean[] logic = new boolean[this.nrow()];
        int which_col1 = this.indexCol(colname1);
        int which_col2 = this.indexCol(colname2);
        int check = 0;
        double[] intsToDbls1 = new double[nrow()];
        double[] intsToDbls2 = new double[nrow()];
        if(!this.colType(which_col1).equals("str")){
            if(this.colType(which_col1).equals("int")){
                for(int i=0; i<nrow(); i++) {
                    intsToDbls1[i] = this.getCol(which_col1).getInts()[i];
                }
            }else if(this.colType(which_col1).equals("dbl")){
                intsToDbls1 = this.getCol(which_col1).getDbls();
            }
            if(this.colType(which_col2).equals("int")){
                for(int i=0; i<nrow(); i++) {
                    intsToDbls2[i] = this.getCol(which_col2).getInts()[i];
                }
            }else if(this.colType(which_col2).equals("dbl")){
                intsToDbls2 = this.getCol(which_col2).getDbls();
            }
            for (int i = 0; i < this.nrow(); ++i) {
                if (intsToDbls1[i] == intsToDbls2[i]) {
                    logic[i] = true;
                    check++;
                } else {
                    logic[i] = false;
                }
            }
        }else if (this.colType(which_col1).equals("str")) {
            for (int i = 0; i < this.nrow(); ++i) {
                if (this.getCol(which_col1).getStrs()[i].equals(this.getCol(which_col2).getStrs()[i])) {
                    logic[i] = true;
                    check++;
                } else {
                    logic[i] = false;
                }
            }
        }
        if (check == 0) {
            System.out.println("Error: No observations meet the request");
            System.exit(0);
        }
        this.filterKey.addCriterion(logic, colname1, ">=",colname2, this.andOrOrNow);
        this.updateFinalFromLast();
        return (this);
    }


    /**
     * Print the DataFrame in a formatted way.
     *
     * @param head The number of rows to print. If the size exceeds the number of rows, whole DataFrame will be printed.
     */
    public void show(int head) {
        int showSize = this.nrow();
        if (head < showSize) showSize = head;
        for (int i = 0; i < showSize; ++i) {
            if (i == 0) {
                for (int j = 0; j < this.ncol(); ++j) {
                    if (j == 0) {
                        System.out.print("\u001B[36m" + "Names:" + "\u001B[0m");
                    }
                    System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getCol(j).getHeader());
                }
                System.out.println("");
                System.out.println("\u001B[36m" + "DataFrame: " + this.nrow() + " × " + this.ncol() + "\u001B[0m");
            }
            for (int j = 0; j < this.ncol(); ++j) {
                if (j == 0) {
                    System.out.print("\u001B[36m" + (i + 1) + ":" + "\u001B[0m");
                }
                if (this.colType(j).equals("dbl")) {
                    System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getCol(j).getDbls()[i]);
                } else if (this.colType(j).equals("str")) {
                    System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getCol(j).getStrs()[i]);
                } else if (this.colType(j).equals("int")) {
                    System.out.print("\u001B[34m" + " / " + "\u001B[0m" + this.getCol(j).getInts()[i]);
                }
            }
            System.out.println("");
        }
        if (showSize < this.nrow())
            System.out.println("\u001B[36m" + (this.nrow() - showSize) + " more row(s)..." + "\u001B[0m");
    }

    /**
     * Print the DataFrame in a formatted way.
     */
    public void show() {
        show(nrow());
    }

    /**
     * Print the DataFrame with column indices.
     *
     * @param head The number of rows to print. If the size exceeds the number of rows, whole DataFrame will be printed.
     */
    public void showWithIndices(int head) {
        int showSize = this.nrow();
        if (head < showSize) showSize = head;
        for (int i = 0; i < showSize; ++i) {
            if (i == 0) {
                for (int j = 0; j < this.ncol(); ++j) {
                    if (j == 0) {
                        System.out.print("\u001B[36m" + "Names:" + "\u001B[0m");
                    }
                    System.out.print("\u001B[34m" + " /" + (j + 1) + " " + "\u001B[0m" + this.getCol(j).getHeader());
                }
                System.out.println("");
                System.out.println("\u001B[36m" + "DataFrame: " + this.nrow() + " × " + this.ncol() + "\u001B[0m");
            }
            for (int j = 0; j < this.ncol(); ++j) {
                if (j == 0) {
                    System.out.print("\u001B[36m" + (i + 1) + ":" + "\u001B[0m");
                }
                if (this.getCol(j).getDbls() != null) {
                    System.out.print("\u001B[34m" + " /" + (j + 1) + "\u001B[0m" + " " + this.getCol(j).getDbls()[i]);
                } else if (this.getCol(j).getStrs() != null) {
                    System.out.print("\u001B[34m" + " /" + (j + 1) + "\u001B[0m" + " " + this.getCol(j).getStrs()[i]);
                } else if (this.getCol(j).getInts() != null) {
                    System.out.print("\u001B[34m" + " /" + (j + 1) + "\u001B[0m" + " " + this.getCol(j).getInts()[i]);
                }
            }
            System.out.println("");
        }
        if (showSize < this.nrow())
            System.out.println("\u001B[36m" + (this.nrow() - showSize) + " more row(s)..." + "\u001B[0m");
    }

    /**
     * Print the DataFrame with column indices.
     */
    public void showWithIndices() {
        showWithIndices(nrow());
    }

    private static void rlocOrg(DataFrame df, int from, int to) {
        from = df.rPos(from);
        to = df.rPos(to);
        for (int j = 0; j < df.ncol(); ++j) {
            int k = 0;
            if (df.getCol(j).getDbls() != null) {
                double[] dbls = new double[to - from];
                for (int i = from; i < to; ++i) {
                    dbls[i] = df.getCol(j).getDbls()[i];
                    k++;
                }
                df.getCol(j).setDblObs(dbls);
            } else if (df.getCol(j).getStrs() != null) {
                String[] strs = new String[to - from];
                for (int i = from; i < to; ++i) {
                    strs[k] = df.getCol(j).getStrs()[i];
                    k++;
                }
                df.getCol(j).setStrObs(strs);
            } else if (df.getCol(j).getInts() != null) {
                int[] ints = new int[to - from];
                for (int i = from; i < to; ++i) {
                    ints[k] = df.getCol(j).getInts()[i];
                    k++;
                }
                df.getCol(j).setIntObs(ints);
            }
        }
    }

    /**
     * Return a DataFrame by given row index.
     *
     * @param chosen The index of the row to select.
     * @return A DataFrame containing the selected row.
     */
    public DataFrame rloc(int chosen) {
        return new DataFrame().clone(this).delFilterKey().delOrderKey().addFilterTrue(chosen, chosen+1,true).exeKeysOrg();
    }

    /**
     * Return a DataFrame by given row indices.
     *
     * @param chosen The indices of the rows to select.
     * @return A DataFrame containing the selected rows.
     */
    public DataFrame rloc(int[] chosen) {
        return new DataFrame().clone(this).delFilterKey().delOrderKey().addFilterTrue(chosen,true).exeKeysOrg();

    }

    /**
     * Return a DataFrame by given row index range.
     *
     * @param from The starting row index (inclusive).
     * @param to The ending row index (exclusive).
     * @return A DataFrame containing the selected rows.
     */
    public DataFrame rloc(int from, int to) {
        return new DataFrame().clone(this).delFilterKey().delOrderKey().addFilterTrue(from, to,true).exeKeysOrg();

    }

    /**
     * Return a DataFrame excluding the given row index.
     *
     * @param chosen The index of the row to exclude.
     * @return A DataFrame containing all rows except the selected one.
     */
    public DataFrame rlocExcept(int[] chosen) {
        return new DataFrame().clone(this).delFilterKey().delOrderKey().addFilterFalse(chosen,true).exeKeysOrg();
    }

    /**
     * Return a DataFrame excluding the given row index.
     *
     * @param from The starting row index (inclusive).
     * @param to The ending row index (exclusive).
     * @return A DataFrame containing all rows except the selected ones.
     */
    public DataFrame rlocExcept(int from, int to) {
        return new DataFrame().clone(this).delFilterKey().delOrderKey().addFilterFalse(from, to,true).exeKeysOrg();
    }

    private static void rlocOrg(DataFrame df, int[] chosen) {
        int k = 0;
        for (int m = 0; m < chosen.length; m++) {
            chosen[m] = df.rPos(chosen[m]);
        }
        for (int j = 0; j < df.ncol(); ++j) {
            k = 0;
            if (df.colType(j).equals("dbl")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    for (int m = 0; m < chosen.length; m++) {
                        if (i == chosen[m]) {
                            df.getCol(j).setDbl(df.getCol(j).getDbls()[i], k);
                            k++;
                        }
                    }
                }
            } else if (df.colType(j).equals("str")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    for (int m = 0; m < chosen.length; m++) {
                        if (i == chosen[m]) {
                            df.getCol(j).setStr(df.getCol(j).getStrs()[i], k);
                            k++;
                        }
                    }
                }
            } else if (df.colType(j).equals("int")) {
                for (int i = 0; i < df.nrow(); ++i) {
                    for (int m = 0; m < chosen.length; m++) {
                        if (i == chosen[m]) {
                            df.getCol(j).setInt(df.getCol(j).getInts()[i], k);
                            k++;
                        }
                    }
                }
            }
            if (k == 0) {
                System.out.println("Error: No observations meet the request");
                System.exit(0);
            }
        }
        rlocOrg(df, 0, k);
    }

    /**
     * Add a given column to a DataFrame.
     *
     * @param col The Column to add.
     * @param where The index to add the Column.
     * @return The DataFrame after adding the Column.
     */
    public DataFrame addCol(Column col, String where) {
        this.columns.add(indexCol(where), col);
        return this;
//        Column[] cols = new Column[this.ncol() + 1];
//        int which = this.indexCol(where);
//        for (int i = 0; i < which; ++i) {
//            cols[i] = this.getCol(i);
//        }
//        cols[which] = col;
//        for (int i = which + 1; i < this.ncol() + 1; ++i) {
//            cols[i] = this.getCol(i-1);
//        }
//        return (new DataFrame(cols).statusClone(this));
    }

    /**
     * Add a given column to a DataFrame.
     *
     * @param col The Column to add.
     * @param where The index to add the Column.
     * @return The DataFrame after adding the Column.
     */
    public DataFrame addCol(Column col, int where) {
        this.columns.add(cPos(where), col);
        return this;
//        Column[] cols = new Column[this.ncol() + 1];
//        where = this.cPos(where);
//        for (int i = 0; i < where; ++i) {
//            cols[i] = this.getCol(i);
//        }
//        cols[where] = col;
//        for (int i = where + 1; i < this.ncol() + 1; ++i) {
//            cols[i] = this.getCol(i-1);
//        }
//        return (new DataFrame(cols).statusClone(this));
    }

    /**
     * Delete a given column from a DataFrame.
     *
     * @param which The name of the column to delete.
     * @return The DataFrame after deleting the Column.
     */
    public DataFrame delCol(String which) {
        this.columns.remove(indexCol(which));
        return this;
//        Column[] cols = new Column[this.ncol() - 1];
//        int where = this.indexCol(which);
//        for (int i = 0; i < where; ++i) {
//            cols[i] = this.getCol(i);
//        }
//        for (int i = where + 1; i < this.ncol(); ++i) {
//            cols[i - 1] = this.getCol(i);
//        }
//        return (new DataFrame(cols).statusClone(this));
    }

    /**
     * Delete a column from a DataFrame.
     *
     * @param which The index of the column to delete.
     * @return The DataFrame after deleting the Column.
     */
    public DataFrame delCol(int which) {
        this.columns.remove(cPos(which));
        return this;
//        which = this.cPos(which);
//        Column[] cols = new Column[this.ncol() - 1];
//        for (int i = 0; i < which; ++i) {
//            cols[i] = this.getCol(i);
//        }
//        for (int i = which + 1; i < this.ncol(); ++i) {
//            cols[i - 1] = this.getCol(i);
//        }
//        return (new DataFrame(cols).statusClone(this));
    }

    /**
     * Set a given column to a DataFrame.
     *
     * @param col The Column to set.
     * @param where The index to set the Column.
     * @return The DataFrame after setting the Column.
     */
    public DataFrame setCol(Column col, int where) {
        this.columns.set(cPos(where), col);
        return (this);
    }

    /**
     * Set a given column to a DataFrame.
     * 
     * @param col The Column to set.
     * @param where The name to set the Column.
     * @return The DataFrame after setting the Column.
     */
    public DataFrame setCol(Column col, String where) {
        return (this.setCol(col, this.indexCol(where)));
    }

    /**
     * Move a column to the given index.
     *
     * @param colname The name of the column to move.
     * @param to The index to move to.
     * @return The DataFrame after moving the Column.
     */
    public DataFrame movCol(String colname, int to) {
        Column[] cols = new Column[this.ncol()];
        to = this.cPos(to);
        boolean moveFront = true;
        int from = this.indexCol(colname);
        if (from < to) moveFront = false;
        if (moveFront) {
            for (int i = 0; i < this.ncol(); i++) {
                if (i >= to && i < from) {
                    cols[i + 1] = this.getCol(i);
                } else {
                    cols[i] = this.getCol(i);
                }
            }
        } else {
            for (int i = 0; i < this.ncol(); i++) {
                if (i > from && i <= to) {
                    cols[i - 1] = this.getCol(i);
                } else {
                    cols[i] = this.getCol(i);
                }
            }
        }
        cols[to] = this.getCol(colname);
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
        return (this);
    }

    /**
     * Move a column to the given index.
     * 
     * @param which The index of the column to move.
     * @param to The index to move to.
     * @return The DataFrame after moving the Column.
     */
    public DataFrame movCol(int which, int to) {
        return (this.movCol(this.getColname(which), to));
    }

    /**
     * Exchange the columns by given column names.
     *
     * @param colname1 The first column name.
     * @param colname2 The second column name.
     * @return The DataFrame after exchanging the Column.
     */
    public DataFrame swapCol(String colname1, String colname2) {
        Column col1 = new Column().clone(this.getCol(colname1));
        Column col2 = new Column().clone(this.getCol(colname2));
        this.setCol(col1, colname2);
        this.setCol(col2, colname1);
        return this;
//        int where1 = this.indexCol(colname1);
//        int where2 = this.indexCol(colname2);
//        Column[] cols = new Column[this.ncol()];
//        for (int i = 0; i < this.ncol(); i++) {
//            if (i != where1 && i != where2) {
//                cols[i] = this.getCol(i);
//            }
//        }
//        cols[where1] = this.getCol(where2);
//        cols[where2] = this.getCol(where1);
//        return (new DataFrame(cols).statusClone(this));
    }
    /**
     * Exchange the columns by given column indices.
     *
     * @param which1 The first column index.
     * @param which2 The second column index.
     * @return The DataFrame after exchanging the Column.
     */
    public DataFrame swapCol(int which1, int which2) {
        return (this.swapCol(this.getColname(which1), this.getColname(which2)));
    }

    /**
     * Set OrderKey by moving a row to the given index.
     *
     * @param which The row index to move.
     * @param to The index to move to.
     * @return The DataFrame after setting the OrderKey.
     */
    public DataFrame setOKeyMovRow(int which, int to){
        which = this.rPos(which);
        to = this.rPos(to);
        this.orderKey.movRow(nrow(),which,to);
        return this;
    }


    /**
     * Add indices starting from 1 as the first column of a DataFrame. It may be useful when achieving the same effect as key function.
     *
     * @return The DataFrame with indices as the first column.
     */
    public DataFrame addIndex() {
        addIndex(1);
        return this;
    }

    /**
     * Add indices as the first column of a DataFrame. It may be useful when achieving the same effect as key function.
     *
     * @param startsFrom The number from which the indices start.
     * @return The DataFrame with indices as the first column.
     */
    public DataFrame addIndex(int startsFrom) {
//        Column[] cols = new Column[this.ncol() + 1];
        int[] index = new int[this.nrow()];
        for (int i = startsFrom; i < this.nrow()+startsFrom; ++i) {
            index[i-startsFrom] = i;
        }
        this.addCol(new Column("Index", index), 0);
        return this;
//        cols[0] = new Column("Index", index);
//        for (int i = 1; i < cols.length; ++i) {
//            cols[i] = this.getCol(i-1);
//        }
//        return (new DataFrame(cols).statusClone(this));
    }

    /**
     * Rename a column of a DataFrame.
     *
     * @param oldName The old column name.
     * @param newName The new column name.
     * @return The original DataFrame after renaming.
     */
    public DataFrame rename(String oldName, String newName) {
        this.getCol(this.indexCol(oldName)).setHeader(newName);
        return (this);
    }
    /** Rename a column of a DataFrame.
     *
     * @param oldOne The old column index.
     * @param newName The new column name.
     * @return The original DataFrame after renaming.
     */
    public DataFrame rename(int oldOne, String newName) {
        this.getCol(oldOne).setHeader(newName);
        return (this);
    }

    /**
     * Rename columns of a DataFrame.
     *
     * @param oldName The old column names.
     * @param newName The new column names.
     * @return The original DataFrame after renaming.
     */
    public DataFrame rename(String[] oldName, String[] newName) {
        for (int i = 0; i < oldName.length; ++i) {
            this.getCol(this.indexCol(oldName[i])).setHeader(newName[i]);
        }
        return (this);
    }

    /**
     * Rename columns of a DataFrame.
     *
     * @param oldOne The old column indices.
     * @param newName The new column names.
     * @return The original DataFrame after renaming.
     */
    public DataFrame rename(int[] oldOne, String[] newName) {
        for (int i = 0; i < oldOne.length; ++i) {
            this.getCol(oldOne[i]).setHeader(newName[i]);
        }
        return (this);
    }

    /**
     * Combine two DataFrames with same column names vertically.
     *
     * @param df2 The DataFrame to combine with.
     * @return The DataFrame after combining.
     */
    public DataFrame rbind(DataFrame df2) {
        if(!Arrays.equals(this.getAllColnames(),df2.getAllColnames())){
            System.out.println("Error: column names of two dataframes are not equal");
            System.exit(0);
        }
        Column[] cols = new Column[this.ncol()];
        for (int i = 0; i < this.ncol(); ++i) {
            cols[i] = this.getCol(this.getColname(i)).rbind(df2.getCol(df2.getColname(i)));
        }
        return (new DataFrame(cols));
    }

    /**
     * Combine two DataFrames with same number of rows horizontally.
     *
     * @param df2 The DataFrame to combine with.
     * @return The DataFrame after combining.
     */
    public DataFrame cbind(DataFrame df2) {
        Column[] cols = new Column[this.ncol() + df2.ncol()];
        for (int i = 0; i < this.ncol(); ++i) {
            cols[i] = this.getCol(this.getColname(i));
        }
        for (int i = this.ncol(); i < cols.length; ++i) {
            cols[i] = df2.getCol(df2.getColname(i - this.ncol()));
        }
        return (new DataFrame(cols).statusClone(this));
    }

//    public DataFrame setFKeyMerge(DataFrame df2, String byColname) {
//        boolean[] logic = new boolean[this.nrow()];
//        for (int i = 0; i < this.nrow(); i++) {
//            logic[i] = false;
//        }
//        Column col1 = this.getCol(byColname);
//        Column col2 = df2.getCol(byColname);
//        if (this.colType(byColname).equals("str")) {
//            for (int i = 0; i < this.nrow(); i++) {
//                for (int j = 0; j < df2.nrow(); j++) {
//                    if (col1.getStrs()[i].equals(col2.getStrs()[j])) {
//                        logic[i] = true;
//                    }
//                }
//            }
//        } else if (this.colType(byColname).equals("int")) {
//            for (int i = 0; i < this.nrow(); i++) {
//                for (int j = 0; j < df2.nrow(); j++) {
//                    if (col1.getInts()[i] == col2.getInts()[j]) {
//                        logic[i] = true;
//                    }
//                }
//            }
//        }
//        this.filterKey.addCriterion(logic,this.andOrOrNow);
//        return (this);
//    }

    /**
     * Sort a DataFrame based on the OrderKey.
     *
     * @return A DataFrame after ordering.
     */
    public DataFrame order() {
        if (this.orderKey.getValues() == null) {
            int[] order = new int[this.nrow()];
            for (int i = 0; i < this.nrow(); i++) {
                order[i] = i;
            }
            this.orderKey.setValues(order);
        }
        int[] order = this.orderKey.getValues();
        Column[] cols = new Column[this.ncol()];
        for (int j = 0; j < this.ncol(); j++) {
            Column col = new Column();
            col.setHeader(this.getCol(j).getHeader());
            if (this.getCol(j).getDbls() != null) {
                col.setDblObs(new double[order.length]);
                for (int i = 0; i < this.nrow(); i++) {
                    col.setDbl(this.getCol(j).getDbls()[order[i]], i);
                }
            } else if (this.getCol(j).getStrs() != null) {
                col.setStrObs(new String[order.length]);
                for (int i = 0; i < this.nrow(); i++) {
                    col.setStr(this.getCol(j).getStrs()[order[i]], i);
                }
            } else if (this.getCol(j).getInts() != null) {
                col.setIntObs(new int[order.length]);
                for (int i = 0; i < this.nrow(); i++) {
                    col.setInt(this.getCol(j).getInts()[order[i]], i);
                }
            }
            cols[j] = col;
        }
        DataFrame df = new DataFrame();
        df.statusClone(this);
        df.columns = new ArrayList<>();
        df.columns.addAll(Arrays.asList(cols));
        return (df);
    }

    private static void orderOrg(DataFrame df) {
        if (df.orderKey.getValues() == null) {
            int[] order = new int[df.nrow()];
            for (int i = 0; i < df.nrow(); i++) {
                order[i] = i;
            }
            df.orderKey.setValues(order);
        }
        int[] order = df.orderKey.getValues();
        Column[] cols = new Column[df.ncol()];
        for (int j = 0; j < df.ncol(); j++) {
            Column col = new Column();
            col.setHeader(df.getCol(j).getHeader());
            if (df.getCol(j).getDbls() != null) {
                col.setDblObs(new double[order.length]);
                for (int i = 0; i < df.nrow(); i++) {
                    col.setDbl(df.getCol(j).getDbls()[order[i]], i);
                }
            } else if (df.getCol(j).getStrs() != null) {
                col.setStrObs(new String[order.length]);
                for (int i = 0; i < df.nrow(); i++) {
                    col.setStr(df.getCol(j).getStrs()[order[i]], i);
                }
            } else if (df.getCol(j).getInts() != null) {
                col.setIntObs(new int[order.length]);
                for (int i = 0; i < df.nrow(); i++) {
                    col.setInt(df.getCol(j).getInts()[order[i]], i);
                }
            }
            cols[j] = col;
        }
        df = df.delOrderKey();
        df.columns = new ArrayList<>();
        df.columns.addAll(Arrays.asList(cols));
    }

//    private DataFrame orderByKeyFrom(DataFrame df) {
//        int[] tempKey = this.orderKey == null ? null : this.orderKey.clone();
//        this.orderKey = df.orderKey.clone();
//        DataFrame df1 = this.order();
//        this.orderKey = tempKey == null ? null : tempKey.clone();
//        return (df1);
//    }

//    /**
//     * Order a DataFrame based on the given order.
//     *
//     * @param order The order to sort by.
//     * @return A DataFrame after ordering.
//     */
//    private DataFrame orderByGiven(int[] order) {
//        int[] tempKey = this.orderKey == null ? null : this.orderKey.clone();
//        this.orderKey = order;
//        DataFrame df = this.order();
//        this.orderKey = tempKey == null ? null : tempKey.clone();
//        return (df);
//    }

    /**
     * Set the OrderKey by the given DataFrame.
     *
     * @param df The DataFrame to set the OrderKey from.
     */
    public DataFrame setOrderKeyFrom(DataFrame df) {
        this.orderKey.copy(df.orderKey);
        return this;
    }

    /**
     * Set the OrderKey by the given order.
     *
     * @param order The order to set the OrderKey to.
     * @return The DataFrame after setting the OrderKey.
     */
    public DataFrame setOrderKey(int[] order) {
        this.orderKey.setValues(order);
        this.orderKey.setName("Given");
        return (this);
    }

    /**
     * Set the OrderKey by reversing the current OrderKey or the original order.
     *
     * @return The DataFrame after setting the OrderKey.
     */
    public DataFrame setOKeyReverse() {
        this.orderKey.reverse(nrow());
        return (this);
    }

    /**
     * Set the OrderKey to the given column name.
     *
     * @param colname The column name to set the OrderKey to.
     * @return The DataFrame after setting the OrderKey.
     */
    public DataFrame setOrderKey(String colname) {
        if (this.colType(colname).equals("dbl")) {
            double[] arr = this.getCol(this.indexCol(colname)).getDbls();
            Integer[] indices = new Integer[arr.length];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = i;
            }
            Arrays.sort(indices, Comparator.comparingDouble(index -> arr[index]));
            int[] sortedIndices = new int[arr.length];
            for (int i = 0; i < sortedIndices.length; i++) {
                sortedIndices[i] = indices[i];
            }
            this.orderKey.setValues(sortedIndices);
        } else if (this.colType(colname).equals("int")) {
            int[] arr = this.getCol(this.indexCol(colname)).getInts();
            Integer[] indices = new Integer[arr.length];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = i;
            }
            Arrays.sort(indices, Comparator.comparingDouble(index -> arr[index]));
            int[] sortedIndices = new int[arr.length];
            for (int i = 0; i < sortedIndices.length; i++) {
                sortedIndices[i] = indices[i];
            }
            this.orderKey.setValues(sortedIndices);
        }
        this.orderKey.setName(colname);
        return (this);
    }

    /**
     * Set the OrderKey to the given column.
     *
     * @param which The column index to set the OrderKey to.
     * @return The DataFrame after setting the OrderKey.
     */
    public DataFrame setOrderKey(int which) {
        return (this.setOrderKey(this.getColname(which)));
    }

//    /**
//     * Get the OrderKey.
//     *
//     * @return The OrderKey.
//     */
//    public int[] getOrderKey() {
//        return (this.orderKey);
//    }

    /**
     * Delete the OrderKey.
     *
     * @return The DataFrame after deleting the OrderKey.
     */
    public DataFrame delOrderKey() {
        this.orderKey.clear();
        return (this);
    }

    /**
     * Fill the nulls in a double Column by the given double value.
     *
     * @param colname The column to fill.
     * @param given  The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(String colname, double given) {
        Column col = this.getCol(colname);
        for (int i = 0; i < this.nrow(); i++) {
            if (Double.isNaN(col.getDbls()[i])) {
                col.setDbl(given, i);
            }
        }
        return (this.setCol(col, colname));
    }

    /**
     * Fill the nulls in a double Column by the given double value.
     *
     * @param which The column index to fill.
     * @param given The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(int which, double given) {
        return (this.filledBy(this.getColname(this.cPos(which)), given));
    }

    /**
     * Fill the nulls in an int Column by the given int value.
     *
     * @param colname The column to fill.
     * @param given  The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(String colname, int given) {
        Column col = this.getCol(colname);
        for (int i = 0; i < this.nrow(); i++) {
            if (Double.isNaN(col.getInts()[i])) {
                col.setInt(given, i);
            }
        }
        return (this.setCol(col, colname));
    }

    /**
     * Fill the nulls in an int Column by the given int value.
     *
     * @param which The column index to fill.
     * @param given The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(int which, int given) {
        return (this.filledBy(this.getColname(this.cPos(which)), given));
    }

    /**
     * Fill the nulls in a string Column by the given string value.
     *
     * @param colname The column to fill.
     * @param given  The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(String colname, String given) {
        Column col = this.getCol(colname);
        for (int i = 0; i < this.nrow(); i++) {
            if (col.getStrs()[i] == null) {
                col.setStr(given, i);
            }
        }
        return (this.setCol(col, colname));
    }

    /**
     * Fill the nulls in a string Column by the given string value.
     *
     * @param which The column index to fill.
     * @param given The given value.
     * @return A DataFrame after filling.
     */
    public DataFrame filledBy(int which, String given) {
        return (this.filledBy(this.getColname(this.cPos(which)), given));
    }

    /**
     * Fill the nulls in a Column by the previous data point.
     *
     * @param colname The column to fill.
     * @return A DataFrame after filling.
     */
    public DataFrame filled(String colname) {
        Column col = this.getCol(colname);
        if (this.colType(colname).equals("dbl")) {
            if (Double.isNaN(col.getDbls()[0])) {
                System.out.println("Error: The first value is NaN");
                System.exit(0);
            }
            for (int i = 1; i < this.nrow(); i++) {
                if (Double.isNaN(col.getDbls()[i])) {
                    col.setDbl(col.getDbls()[i - 1], i);
                }
            }
        } else if (this.colType(colname).equals("str")) {
            for (int i = 1; i < this.nrow(); i++) {
                if (col.getStrs()[i] == null) {
                    col.setStr(col.getStrs()[i - 1], i);
                }
            }
        } else if (this.colType(colname).equals("int")) {
            if (Double.isNaN(col.getInts()[0])) {
                System.out.println("Error: The first value is NaN");
                System.exit(0);
            }
            for (int i = 1; i < this.nrow(); i++) {
                if (Double.isNaN(col.getInts()[i])) {
                    col.setInt(col.getInts()[i - 1], i);
                }
            }
        }
        return (this.setCol(col, colname));
    }

    /**
     * Fill the nulls in a Column by the previous data point.
     *
     * @param which The column index to fill.
     * @return A DataFrame after filling.
     */
    public DataFrame filled(int which) {
        return (this.filled(this.getColname(this.cPos(which))));
    }

    /**
     * Show the unique values in an int Column.
     *
     * @param colname The column name to show unique values.
     * @return An array of unique values.
     */
    public int[] intUnique(String colname) {
        return (IntStream.of(this.getColInts(colname)).distinct().toArray());
    }

    /**
     * Show the unique values in an int Column.
     *
     * @param which The column index to show unique values.
     * @return An array of unique values.
     */
    public int[] intUnique(int which) {
        return (IntStream.of(this.getColInts(this.cPos(which))).distinct().toArray());
    }

    /**
     * Show the unique values in a string Column.
     *
     * @param colname The column name to show unique values.
     * @return An array of unique values.
     */
    public String[] strUnique(String colname) {
        return (Arrays.stream(this.getColStrs(colname)).distinct().toArray(String[]::new));
    }

    /**
     * Show the unique values in a string Column.
     *
     * @param which The column index to show unique values.
     * @return An array of unique values.
     */
    public String[] strUnique(int which) {
        return (Arrays.stream(this.getColStrs(this.cPos(which))).distinct().toArray(String[]::new));
    }

//    private DataFrame sumBy(String colname, String byColname) {
//        DataFrame df = new DataFrame().clone(this).delAllKeys().setGroupKey(byColname).sum(colname);
//        df.delAllKeys();
//        return (df);
//    }
//
//    private DataFrame sum(String colname) {
//        DataFrame df = new DataFrame();
//        if (this.colType(colname).equals("dbl")) {
//            double[] values = this.getColDbls(colname);
//            double[][] sumData = new double[1][1];
//            if (this.groupKey == null) {
//                for (int i = 0; i < this.nrow(); i++) {
//                    sumData[0][0] += values[i];
//                }
//                df = new DataFrame(new String[]{"Sum"}, sumData);
//            } else if (this.groupKeyType.equals("str")) {
//                String[] unique = this.strUnique(this.groupKey);
//                double[] sumData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locDbl(i, this.indexCol(colname));
//                    }
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Sum", sumData1));
//            } else if (this.groupKeyType.equals("int")) {
//                int[] unique = this.intUnique(this.groupKey);
//                double[] sumData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locDbl(i, this.indexCol(colname));
//                    }
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Sum", sumData1));
//            }
//        } else if (this.colType(colname).equals("int")) {
//            int[] values = this.getColInts(colname);
//            int sum = 0;
//            int[][] sumData = new int[1][1];
//            if (this.groupKey == null) {
//                for (int i = 0; i < this.nrow(); i++) {
//                    sum += values[i];
//                }
//                sumData[0][0] = sum;
//                df = new DataFrame(new String[]{"Sum"}, sumData);
//            } else if (this.groupKeyType.equals("str")) {
//                String[] unique = this.strUnique(this.groupKey);
//                int[] sumData1 = new int[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locInt(i, this.indexCol(colname));
//                    }
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Sum", sumData1));
//            } else if (this.groupKeyType.equals("int")) {
//                int[] unique = this.intUnique(this.groupKey);
//                int[] sumData1 = new int[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locInt(i, this.indexCol(colname));
//                    }
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Sum", sumData1));
//            }
//        }
//        return (df);
//    }
//
//    private DataFrame sum(int which) {
//        return (this.sum(this.getColname(this.cPos(which))));
//    }
//
//    private DataFrame averageBy(String colname, String byColname) {
//        DataFrame df = new DataFrame().clone(this).delAllKeys().setGroupKey(byColname).average(colname);
//        df.delAllKeys();
//        return (df);
//    }
//
//    private DataFrame average(String colname) {
//        DataFrame df = new DataFrame();
//        if (this.colType(colname).equals("dbl")) {
//            double[] values = this.getColDbls(colname);
//            double sum;
//            double[][] avrgData = new double[1][1];
//            if (this.groupKey == null) {
//                sum = 0;
//                for (int i = 0; i < this.nrow(); i++) {
//                    sum += values[i];
//                }
//                avrgData[0][0] = sum / this.nrow();
//                df = new DataFrame(new String[]{"Average"}, avrgData);
//            } else if (this.groupKeyType.equals("str")) {
//                String[] unique = this.strUnique(this.groupKey);
//                double[] sumData1 = new double[unique.length];
//                double[] avrgData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locDbl(i, this.indexCol(colname));
//                    }
//                    avrgData1[j] = sumData1[j] / this.uniqueCountDF().locInt(j, 1);
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Avrg.", avrgData1));
//            } else if (this.groupKeyType.equals("int")) {
//                int[] unique = this.intUnique(this.groupKey);
//                double[] sumData1 = new double[unique.length];
//                double[] avrgData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locDbl(i, this.indexCol(colname));
//                    }
//                    avrgData1[j] = sumData1[j] / this.uniqueCountDF().locInt(j, 1);
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Avrg.", avrgData1));
//            }
//        } else if (this.colType(colname).equals("int")) {
//            int[] values = this.getColInts(colname);
//            int sum = 0;
//            double[][] avrgData = new double[1][1];
//            if (this.groupKey == null) {
//                for (int i = 0; i < this.nrow(); i++) {
//                    sum += values[i];
//                }
//                avrgData[0][0] = sum / this.nrow();
//                df = new DataFrame(new String[]{"Average"}, avrgData);
//            } else if (this.groupKeyType.equals("str")) {
//                String[] unique = this.strUnique(this.groupKey);
//                int[] sumData1 = new int[unique.length];
//                double[] avrgData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locInt(i, this.indexCol(colname));
//                    }
//                    avrgData1[j] = sumData1[j] / this.uniqueCountDF().locInt(j, 1);
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Avrg.", avrgData1));
//            } else if (this.groupKeyType.equals("int")) {
//                int[] unique = this.intUnique(this.groupKey);
//                int[] sumData1 = new int[unique.length];
//                double[] avrgData1 = new double[unique.length];
//                for (int j = 0; j < unique.length; j++) {
//                    for (int i = 0; i < this.uniqueCountDF().locInt(j, 1); i++) {
//                        sumData1[j] += this.eq(this.groupKey, unique[j]).locInt(i, this.indexCol(colname));
//                    }
//                    avrgData1[j] = sumData1[j] / this.uniqueCountDF().locInt(j, 1);
//                }
//                df = new DataFrame(new Column("Groups", unique), new Column("Avrg.", avrgData1));
//            }
//        }
//
//        return (df);
//    }
//
//    private DataFrame average(int which) {
//        return (this.average(this.getColname(which)));
//    }

    /**
     * Set the column as the GroupKey. The column type should be "str" or "int".
     *
     * @param colname The column name to set as the GroupKey.
     * @return A DataFrame with the new GroupKey.
     */
    public DataFrame setGroupKey(String colname) {
        if (colname != null) {
            this.groupKey.setValue(colname);
            this.groupKey.setName(colname);
            if (this.colType(colname).equals("str")) {
                this.groupKey.setValueType("str");
            } else if (this.colType(colname).equals("int")) {
                this.groupKey.setValueType("int");
            } else if (this.colType(colname).equals("dbl")) {
                System.out.println("Error: GroupKey cannot be set to a double type column");
                System.exit(0);
            }
        }
        return (this);
    }

    /**
     * Set the column as the GroupKey. The column type should be "str" or "int".
     *
     * @param which The column number to set as the GroupKey.
     * @return A DataFrame with the new GroupKey.
     */
    public DataFrame setGroupKey(int which){
        setGroupKey(getColname(which));
        return this;
    }

//    /**
//     * Get the column set as the GroupKey.
//     *
//     * @return The GroupKey.
//     */
//    public String getGroupKey() {
//        return (this.groupKey);
//    }
//
//    /**
//     * Check the column type of the GroupKey.
//     *
//     * @return The column type of the GroupKey.
//     */
//    public String getGroupKeyType() {
//        return (this.groupKeyType);
//    }

    /**
     * Delete the GroupKey.
     *
     * @return A DataFrame without the GroupKey.
     */
    public DataFrame delGroupKey() {
        this.groupKey.clear();
        return (this);
    }

    /**
     * Count the number of unique values in a column set as the GroupKey.
     *
     * @return A DataFrame with two columns: "Group" and "Count".
     */
    public DataFrame uniqueCountDF() {
        if(this.groupKey.getName() == null){
            System.out.println("Error: Set the groupKey first");
            System.exit(0);
        }
        int count = 0;
        DataFrame df = new DataFrame();
        if (this.groupKey.getValueType().equals("str")) {
            int[] counts = new int[this.strUnique(groupKey.getValue()).length];
            for (int j = 0; j < counts.length; j++) {
                count = 0;
                for (int i = 0; i < this.nrow(); i++) {
                    if (this.strUnique(groupKey.getValue())[j].equals(this.locStr(i, this.indexCol(groupKey.getValue())))) {
                        count++;
                    }
                }
                counts[j] = count;
            }
            String[] names = this.strUnique(groupKey.getValue());
            df = new DataFrame(new Column("Group", names), new Column("Count", counts));
        } else if (this.groupKey.getValueType().equals("int")) {
            int[] counts = new int[this.intUnique(groupKey.getValue()).length];
            int[] unique = this.intUnique(groupKey.getValue());
            String[] uniqueName = new String[unique.length];
            for (int j = 0; j < this.intUnique(groupKey.getValue()).length; j++) {
                count = 0;
                for (int i = 0; i < this.nrow(); i++) {
                    if (unique[j] == this.locInt(i, this.indexCol(groupKey.getValue()))) {
                        count++;
                    }
                }
                counts[j] = count;
            }
            for (int i = 0; i < unique.length; i++) {
                uniqueName[i] = Integer.toString(unique[i]);
            }
            df = new DataFrame(new Column("Group", uniqueName), new Column("Count", counts));
        }
        return (df);
    }

    /**
     * Add new columns for dummy variables to the DataFrame.
     *
     * @return A new DataFrame with new columns for dummy variables.
     */
    public DataFrame addDummyCols() {
        if (this.groupKey.getName() == null) {
            System.out.println("Error: Set the groupKey first");
            System.exit(0);
        }
        DataFrame df = new DataFrame();
        if (this.groupKey.getValueType().equals("str")) {
            String[] unique = this.strUnique(this.groupKey.getValue());
            int[][] strs = new int[unique.length][this.nrow()];
            for (int i = 0; i < strs.length; i++) {
                for (int j = 0; j < this.nrow(); j++) {
                    if (unique[i].equals(this.locStr(j, this.indexCol(groupKey.getValue())))) {
                        strs[i][j] = 1;
                    } else {
                        strs[i][j] = 0;
                    }
                }
            }
            String[] names = new String[unique.length];
            for(int i=0; i<unique.length; i++){
                names[i] = this.groupKey + "-" + unique[i];
            }
            DataFrame dummy = new DataFrame(names, strs);
            df = this.cbind(dummy).delCol(groupKey.getValue()).delGroupKey();
        } else if (this.groupKey.getValueType().equals("int")) {
            int[] unique = this.intUnique(this.groupKey.getValue());
            int[][] ints = new int[unique.length][this.nrow()];
            for (int i = 0; i < ints.length; i++) {
                for (int j = 0; j < this.nrow(); j++) {
                    if (unique[i] == this.locInt(j, this.indexCol(groupKey.getValue()))) {
                        ints[i][j] = 1;
                    } else {
                        ints[i][j] = 0;
                    }
                }
            }
            String[] names = new String[unique.length];
            for(int i=0; i<unique.length; i++){
                names[i] = this.groupKey + "-" + unique[i];
            }
            DataFrame dummy = new DataFrame(names, ints);
            df = this.cbind(dummy).delCol(groupKey.getValue()).delGroupKey();
        }
        return df;
    }

    private static String determineColType(String header, String[] obs) {
        char[] number = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.'};
        int notNum;
        int points;
        boolean missing = false;
        boolean isStr = false;
        boolean isDbl = false;
        boolean isInt = true;
        for (String i : obs) {
            if (!i.isEmpty()) {
                if (isStr) {
                    break;
                }
                points = 0;
                for (int j = 0; j < i.length(); j++) {
                    if (isStr) {
                        break;
                    }
                    notNum = 0;
                    if (i.charAt(j) == '.') {
                        isDbl = true;
                        isStr = false;
                        isInt = false;
                        points++;
                        if (points >= 2) {
                            isDbl = false;
                            isStr = true;
                            isInt = false;
                        }
                    }
                    for (char k : number) {
                        if (i.charAt(j) == k) {
                            break;
                        } else {
                            notNum++;
                        }
                    }
                    if (notNum == 11) {
                        isStr = true;
                        isDbl = false;
                        isInt = false;
                    }
                }
            } else {
                missing = true;
            }
        }
        if (isStr) {
            return "str";
        } else if (isDbl) {
            return "dbl";
        } else if (isInt && missing) {
            System.out.println(
                    "Warning: Integer column \"" + header + "\" has been changed into double type due to missing value existing"
            );
            return "dbl";
        } else {
            return "int";
        }
    }

    /**
     * Read data without headers from a file as a DataFrame. The columns data type should be specified in the parameters.
     *
     * @param file The path of the file.
     * @param nrow The number of rows to read.
     * @param splitBy The separator used to separate columns.
     * @param dblCols The column indices of double columns.
     * @param strCols The column indices of string columns.
     * @param intCols The column indices of integer columns.
     * @throws Exception if any error occurs.
     */
    public void readTableWOH(String file, int nrow, String splitBy, int[] dblCols, int[] strCols, int[] intCols) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        int[][] ints = new int[intCols.length][nrow];
        String[][] strs = new String[strCols.length][nrow];
        double[][] dbls = new double[dblCols.length][nrow];
        String s = bf.readLine();
        String[] ss = s.split(splitBy);
        String[] colnames = new String[ss.length];
        Column[] cols = new Column[ss.length];
        for (int i = 0; i < ss.length; i++) {
            colnames[i] = "Col" + (i + 1);
        }
        for (int k = 0; k < nrow; k++) {
            if (k != 0) {
                s = bf.readLine();
                ss = s.split(splitBy);
            }
            for (int i = 0; i < ss.length; i++) {
                if (intCols.length != 0) {
                    for (int j = 0; j < intCols.length; j++) {
                        if (i == intCols[j]) {
                            ints[j][k] = Integer.parseInt(ss[i]);
                        }
                    }
                }
                if (strCols.length != 0) {
                    for (int j = 0; j < strCols.length; j++) {
                        if (i == strCols[j]) {
                            strs[j][k] = ss[i];
                        }
                    }
                }
                if (dblCols.length != 0) {
                    for (int j = 0; j < dblCols.length; j++) {
                        if (i == dblCols[j]) {
                            try {
                                dbls[j][k] = Double.parseDouble(ss[i]);
                            } catch (Exception e) {
                                dbls[j][k] = Double.NaN;
                            }
                        }
                    }
                }
            }
        }
        for (int i = 0; i < ss.length; i++) {
            if (strCols.length != 0) {
                for (int j = 0; j < strCols.length; j++) {
                    if (i == strCols[j]) {
                        cols[i] = new Column(colnames[i], strs[j]);
                    }
                }
            }
            if (intCols.length != 0) {
                for (int j = 0; j < intCols.length; j++) {
                    if (i == intCols[j]) {
                        cols[i] = new Column(colnames[i], ints[j]);
                    }
                }
            }
            if (dblCols.length != 0) {
                for (int j = 0; j < dblCols.length; j++) {
                    if (i == dblCols[j]) {
                        cols[i] = new Column(colnames[i], dbls[j]);
                    }
                }
            }
        }
        bf.close();
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Read data without headers from a file as a DataFrame. The columns data type would be determined automatically.
     *
     * @param file The path of the file.
     * @param splitBy The separator used to separate columns.
     * @throws Exception if any error occurs.
     */
    public void readTableWOH(String file, String splitBy) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        String s = bf.readLine();
        String[] ss = s.split(splitBy);
        String[] colnames = new String[ss.length];
        Column[] cols = new Column[ss.length];
        for (int i = 0; i < ss.length; i++) {
            colnames[i] = "Col" + (i + 1);
        }
        int nrow = 0;
        while (true) {
            s = bf.readLine();
            if (s == null) {
                break;
            }
            nrow++;
        }
        bf.close();
        String[][] strs = new String[ss.length][nrow];
        FileReader fr2 = new FileReader(file);
        BufferedReader bf2 = new BufferedReader(fr2);
        bf2.readLine();
        for (int j = 0; j < nrow; j++) {
            if (j != 0) {
                s = bf2.readLine();
                ss = s.split(splitBy);
            }
            for (int i = 0; i < ss.length; i++) {
                strs[i][j] = ss[i];
            }
        }
        for (int i = 0; i < ss.length; i++) {
            String type = DataFrame.determineColType(colnames[i], strs[i]);
            if (type.equals("dbl")) {
                double[] dbls = new double[nrow];
                for (int j = 0; j < nrow; j++) {
                    try {
                        dbls[j] = Double.parseDouble(strs[i][j]);
                    } catch (Exception e) {
                        dbls[j] = Double.NaN;
                    }
                }
                cols[i] = new Column(colnames[i], dbls);
            } else if (type.equals("int")) {
                int[] ints = new int[nrow];
                for (int j = 0; j < nrow; j++) {
                    ints[j] = Integer.parseInt(strs[i][j]);
                }
                cols[i] = new Column(colnames[i], ints);
            } else {
                String[] str = new String[nrow];
                for (int j = 0; j < nrow; j++) {
                    str[j] = strs[i][j];
                }
                cols[i] = new Column(colnames[i], str);
            }
        }
        bf.close();
        bf2.close();
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Read data from a file as a DataFrame. The columns data type would be determined automatically.
     *
     * @param file The path of the file.
     * @param splitBy The separator used to separate columns.
     * @throws Exception if any error occurs.
     */
    public void readTable(String file, String splitBy) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        String s = bf.readLine();
        String[] ss = s.split(splitBy);
        String[] colnames = new String[ss.length];
        Column[] cols = new Column[ss.length];
        for (int i = 0; i < ss.length; i++) {
            colnames[i] = ss[i];
        }
        int nrow = 0;
        while (true) {
            s = bf.readLine();
            if (s == null) {
                break;
            }
            nrow++;
        }
        bf.close();
        String[][] strs = new String[ss.length][nrow];
        FileReader fr2 = new FileReader(file);
        BufferedReader bf2 = new BufferedReader(fr2);
        bf2.readLine();
        for (int j = 0; j < nrow; j++) {
            s = bf2.readLine();
            ss = s.split(splitBy);
            for (int i = 0; i < ss.length; i++) {
                strs[i][j] = ss[i];
            }
        }
        for (int i = 0; i < ss.length; i++) {
            String type = DataFrame.determineColType(colnames[i], strs[i]);
            if (type.equals("dbl")) {
                double[] dbls = new double[nrow];
                for (int j = 0; j < nrow; j++) {
                    try {
                        dbls[j] = Double.parseDouble(strs[i][j]);
                    } catch (Exception e) {
                        dbls[j] = Double.NaN;
                    }
                }
                cols[i] = new Column(colnames[i], dbls);
            } else if (type.equals("int")) {
                int[] ints = new int[nrow];
                for (int j = 0; j < nrow; j++) {
                    ints[j] = Integer.parseInt(strs[i][j]);
                }
                cols[i] = new Column(colnames[i], ints);
            } else {
                String[] str = new String[nrow];
                for (int j = 0; j < nrow; j++) {
                    str[j] = strs[i][j];
                }
                cols[i] = new Column(colnames[i], str);
            }
        }
        bf.close();
        bf2.close();
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Read data from a file as a DataFrame. The columns data type should be specified in the parameters.
     *
     * @param file The path of the file.
     * @param nrow The number of rows to read.
     * @param splitBy The separator used to separate columns.
     * @param dblCols The column indices of double columns.
     * @param strCols The column indices of string columns.
     * @param intCols The column indices of integer columns.
     * @throws Exception if any error occurs.
     */
    public void readTable(String file, int nrow, String splitBy, int[] dblCols, int[] strCols, int[] intCols) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        String s = bf.readLine();
        String[] ss = s.split(splitBy);
        int[][] ints = new int[intCols.length][nrow];
        String[][] strs = new String[strCols.length][nrow];
        double[][] dbls = new double[dblCols.length][nrow];
        String[] colnames = new String[ss.length];
        Column[] cols = new Column[ss.length];
        for (int i = 0; i < ss.length; i++) {
            colnames[i] = ss[i];
        }
        for (int k = 0; k < nrow; k++) {
            s = bf.readLine();
            ss = s.split(splitBy);
            for (int i = 0; i < ss.length; i++) {
                if (intCols.length != 0) {
                    for (int j = 0; j < intCols.length; j++) {
                        if (i == intCols[j]) {
                            ints[j][k] = Integer.parseInt(ss[i]);
                        }
                    }
                }
                if (strCols.length != 0) {
                    for (int j = 0; j < strCols.length; j++) {
                        if (i == strCols[j]) {
                            strs[j][k] = ss[i];
                        }
                    }
                }
                if (dblCols.length != 0) {
                    for (int j = 0; j < dblCols.length; j++) {
                        if (i == dblCols[j]) {
                            try {
                                dbls[j][k] = Double.parseDouble(ss[i]);
                            } catch (Exception e) {
                                dbls[j][k] = Double.NaN;
                            }
                        }
                    }
                }
            }
        }
        for (int i = 0; i < ss.length; i++) {
            if (strCols.length != 0) {
                for (int j = 0; j < strCols.length; j++) {
                    if (i == strCols[j]) {
                        cols[i] = new Column(colnames[i], strs[j]);
                    }
                }
            }
            if (intCols.length != 0) {
                for (int j = 0; j < intCols.length; j++) {
                    if (i == intCols[j]) {
                        cols[i] = new Column(colnames[i], ints[j]);
                    }
                }
            }
            if (dblCols.length != 0) {
                for (int j = 0; j < dblCols.length; j++) {
                    if (i == dblCols[j]) {
                        cols[i] = new Column(colnames[i], dbls[j]);
                    }
                }
            }
        }
        bf.close();
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Read data from a file as a DataFrame. The columns data type should be specified in the parameters.
     *
     * @param file The path of the file.
     * @param nrow The number of rows to read.
     * @param splitBy The separator used to separate columns.
     * @param dblCols The column names of double columns.
     * @param strCols The column names of string columns.
     * @param intCols The column names of integer columns.
     * @throws Exception if any error occurs.
     */
    public void readTable(String file, int nrow, String splitBy, String[] dblCols, String[] strCols, String[] intCols) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        String s = bf.readLine();
        String[] ss = s.split(splitBy);
        int[][] ints = new int[intCols.length][nrow];
        String[][] strs = new String[strCols.length][nrow];
        double[][] dbls = new double[dblCols.length][nrow];
        String[] colnames = new String[ss.length];
        Column[] cols = new Column[ss.length];
        for (int i = 0; i < ss.length; i++) {
            colnames[i] = ss[i];
        }
        for (int k = 0; k < nrow; k++) {
            s = bf.readLine();
            ss = s.split(splitBy);
            for (int i = 0; i < ss.length; i++) {
                if (intCols.length != 0) {
                    for (int j = 0; j < intCols.length; j++) {
                        if (ss[i].equals(intCols[j])) {
                            ints[j][k] = Integer.parseInt(ss[i]);
                        }
                    }
                }
                if (strCols.length != 0) {
                    for (int j = 0; j < strCols.length; j++) {
                        if (ss[i].equals(strCols[j])) {
                            strs[j][k] = ss[i];
                        }
                    }
                }
                if (dblCols.length != 0) {
                    for (int j = 0; j < dblCols.length; j++) {
                        if (ss[i].equals(dblCols[j])) {
                            try {
                                dbls[j][k] = Double.parseDouble(ss[i]);
                            } catch (Exception e) {
                                dbls[j][k] = Double.NaN;
                            }
                        }
                    }
                }
            }
        }
        for (int i = 0; i < ss.length; i++) {
            if (strCols.length != 0) {
                for (int j = 0; j < strCols.length; j++) {
                    if (ss[i].equals(strCols[j])) {
                        cols[i] = new Column(colnames[i], strs[j]);
                    }
                }
            }
            if (intCols.length != 0) {
                for (int j = 0; j < intCols.length; j++) {
                    if (ss[i].equals(intCols[j])) {
                        cols[i] = new Column(colnames[i], ints[j]);
                    }
                }
            }
            if (dblCols.length != 0) {
                for (int j = 0; j < dblCols.length; j++) {
                    if (ss[i].equals(dblCols[j])) {
                        cols[i] = new Column(colnames[i], dbls[j]);
                    }
                }
            }
        }
        bf.close();
        this.columns = new ArrayList<>();
        this.columns.addAll(Arrays.asList(cols));
    }

    /**
     * Write a DataFrame to a file.
     *
     * @param filename The path of the file.
     * @param splitBy  The separator used to separate columns.
     */
    public void writeTable(String filename, String splitBy) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (int j = 0; j < this.ncol() - 1; j++) {
                writer.write(String.valueOf(this.getColname(j)) + splitBy);
            }
            writer.write(String.valueOf(this.getColname(-1)));
            writer.newLine();
            for (int i = 0; i < this.nrow(); i++) {
                for (int j = 0; j < this.ncol() - 1; j++) {
                    if (this.colType(j).equals("dbl")) {
                        writer.write(String.valueOf(this.locDbl(i, j)) + splitBy);
                    } else if (this.colType(j).equals("str")) {
                        writer.write(this.locStr(i, j) + splitBy);
                    } else if (this.colType(j).equals("int")) {
                        writer.write(String.valueOf(this.locInt(i, j)) + splitBy);
                    }
                }
                if (this.colType(-1).equals("dbl")) {
                    writer.write(String.valueOf(this.locDbl(i, -1)));
                } else if (this.colType(-1).equals("str")) {
                    writer.write(this.locStr(i, -1));
                } else if (this.colType(-1).equals("int")) {
                    writer.write(String.valueOf(this.locInt(i, -1)));
                }
                writer.newLine();
            }
            System.out.println("Data has been written to " + filename);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    /**
     * Deep copy a DataFrame.
     *
     * @param old The DataFrame to be cloned.
     * @return A new DataFrame with the same data as the old one.
     */
    public DataFrame clone(DataFrame old){
        this.columns = new ArrayList<>();
        for(int i=0; i<old.columns.size(); i++){
            this.columns.add(i, new Column());
            this.getCol(i).clone(old.getCol(i));
        }
        this.setFilterKeyFrom(old,false);
        this.setOrderKeyFrom(old);
        return this;
    }

    /**
     * Clone the key status of a DataFrame.
     *
     * @param old The DataFrame to be cloned.
     * @return A new DataFrame with the same status as the old one.
     */
    public DataFrame statusClone(DataFrame old){
        this.setFilterKeyFrom(old,false);
        this.setOrderKeyFrom(old);
        return this;
    }

    private int rPos(int i) {
        int ind = i < 0 ? this.nrow() + i : i;
        if(ind >= this.nrow()){
            throw new IndexOutOfBoundsException("Index: " + i + ", Row Size: " + this.nrow());
        }
        return (ind);
    }

    private int cPos(int i) {
        int ind = i < 0 ? this.ncol() + i : i;
        if(ind >= this.ncol()){
            throw new IndexOutOfBoundsException("Index: " + i + ", Column Size: " + this.ncol());
        }
        return (ind);
    }

    private static boolean[] ifContains(int[] arr, int[] nums, boolean byTrue){
        boolean[] contains = new boolean[arr.length];
        if(byTrue){
            for (int i=0; i<arr.length; i++) {
                int count = 0;
                for(int j=0; j<nums.length; j++){
                    count++;
                    if(arr[i] == nums[j]){contains[i] = true; break;}
                    else if(count == nums.length){contains[i] = false;}
                }
            }
        }else{
            for (int i=0; i<arr.length; i++) {
                int count = 0;
                for(int j=0; j<nums.length; j++){
                    count++;
                    if(arr[i] == nums[j]){contains[i] = false; break;}
                    else if(count == nums.length){contains[i] = true;}
                }
            }
        }
        return contains;
    }

    /**
     * Check if two DataFrames are equal.
     *
     * @param o The object to be compared.
     * @return True if the two DataFrames are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFrame dataFrame = (DataFrame) o;
        return Objects.equals(columns, dataFrame.columns) && Objects.equals(filterKey, dataFrame.filterKey) && Objects.deepEquals(finalFilterValues, dataFrame.finalFilterValues) && Objects.equals(groupKey, dataFrame.groupKey) && Objects.equals(orderKey, dataFrame.orderKey) && Objects.equals(andOrOrNow, dataFrame.andOrOrNow);
    }

    /**
     * Get the hash code of a DataFrame.
     *
     * @return The hash code of the DataFrame.
     */
    @Override
    public int hashCode() {
        return Objects.hash(columns, filterKey, Arrays.hashCode(finalFilterValues), groupKey, orderKey, andOrOrNow);
    }
}
