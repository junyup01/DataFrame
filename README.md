# DataFrame
Java DataFrame library for basic data processing

# Usage
To build a project with this library, add the following to your project’s `pom.xml`
```
<dependencies>
    <dependency>
        <groupId>com.jvcats</groupId>
        <artifactId>dataframe</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```
  
The base package currently contains basic functionality for DataFrame and supports double, string, and integer data.
It uses "key" to store some information such as filter, order, and group for DataFrame, one can reuse the keys on another DataFrame.  
Let's take a quick look at how to use it: 
```
// Create columns to build a DataFrame
// Avoid spaces in column names.
Column names = new Column("Name", "Amy", "Ben", "Cassie", "David");
Column ages = new Column("Age", 20, 25, 21, 24);
Column heights = new Column("Height", 160., 170., 165., 180.);
Column scores1 = new Column("Math", 40, 60, 80, 80);
Column scores2 = new Column("English", 60, 60, 70, 90);
DataFrame df0 = new DataFrame(names, ages, heights, scores1, scores2);
df0.show();

// Write and read DataTable, also can specify column type if you want
String file = "your/path/file1.csv";
df0.writeTable(file, ",");
DataFrame df1 = new DataFrame();
try {
    df1.readTable(file, ",");
} catch (Exception e) {
    System.out.println("Error: cannot read");
}

// Filter the DataFrame
// Below only modify the key setting (FilterKey) of the DataFrame, use filter() to get a new filtered DataFrame
DataFrame df1A = df1.addFilterGt("Age", 20).and()
        .addFilterColGt("Math", "English")
        .filter();

// Modify the original data when dealing with column tasks
df1.addCol(new Column("History", 100, 70, 50, 80), -1);
df1.delCol("English");

// Set the key for ordering and reversing, and get a new ordered DataFrame
DataFrame df1B = df1.setOrderKey("Height")
        .setOKeyReverse()
        .order();

// We now see 3 years later, to check if we can pick the same person and have the same order as previously did
// If we need to select the same rows as in the previous DataFrame, simply set the key from it and filter
Column ages2 = new Column("Age", 23, 28, 24, 27);
Column heights2 = new Column("Height", 165., 170., 170., 180.);
Column scores12 = new Column("Math", 80, 30, 70, 80);
Column scores22 = new Column("History", 90, 40, 70, 100);
DataFrame df2 = new DataFrame(names, ages2, heights2, scores12, scores22);

df2.setFilterKeyFrom(df1, true).filter().show();
df1A.show();

df2.setOrderKeyFrom(df1).order().show();
df1B.show();
```
