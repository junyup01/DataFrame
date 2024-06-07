package com.jvcats.dataframe.base;

import java.util.ArrayList;
import java.util.Objects;

/**
 * This class is used to store the filter keys for DataFrame.
 *
 * @author henry
 */
public class FilterKey {
    private ArrayList<String> names = null;
    private ArrayList<boolean[]> values = null;
    private ArrayList<String> andOrOr = null;

    FilterKey(){}

    FilterKey(ArrayList<String> names, ArrayList<boolean[]> values, ArrayList<String> andOror){
        this.names = names;
        this.values = values;
        this.andOrOr = andOror;
    }

    void addCriterion(boolean[] values, String colname, String logic, String base, String logic2) {
        if (this.values == null) {
            this.values = new ArrayList<boolean[]>();
            this.names = new ArrayList<String>();
            this.andOrOr = new ArrayList<String>();
        }
        this.values.add(values.clone());
        this.names.add(logic2+" "+colname + " " + logic+" "+base);
        this.andOrOr.add(logic2);
    }
    void addCriterion(boolean[] values, String isAndOrOrNow){
        if (this.values == null) {
            this.values = new ArrayList<boolean[]>();
            this.names = new ArrayList<String>();
            this.andOrOr = new ArrayList<String>();
        }
        this.values.add(values.clone());
        this.names.add("Given");
        this.andOrOr.add(isAndOrOrNow);
    }

    void addCriterion(int[] keyForTrues, int nrow, String isAndOrOrNow) {
        int[] indices = new int[nrow];
        for(int i=0; i<nrow; i++){
            indices[i] = i;
        }
        boolean[] value = ifContains(indices, keyForTrues, true);
        this.addCriterion(value, isAndOrOrNow);
    }
    void addCriterion(int keyForTruesFrom, int keyForTruesTo, int nrow, String isAndOrOrNow) {
        keyForTruesFrom = keyForTruesFrom < 0? nrow + keyForTruesFrom : keyForTruesFrom;
        keyForTruesTo = keyForTruesTo < 0? nrow : keyForTruesTo;
        int[] nums = new int[keyForTruesTo-keyForTruesFrom];
        int[] indices = new int[nrow];
        for(int i=0; i<nrow; i++){
            indices[i] = i;
        }
        for(int i=keyForTruesFrom; i<keyForTruesTo; i++){
            nums[i-keyForTruesFrom] = i;
        }
        boolean[] value = ifContains(indices, nums, true);
        this.addCriterion(value, isAndOrOrNow);
    }

    void addValuesByFalse(int[] keyForFalses, int nrow, String isAndOrOrNow) {
        int[] indices = new int[nrow];
        for(int i=0; i<nrow; i++){
            indices[i] = i;
        }
        boolean[] value = ifContains(indices, keyForFalses, false);
        this.addCriterion(value, isAndOrOrNow);
    }
    void addValuesByFalse(int keyForFalsesFrom, int keyForTruesTo, int nrow, String isAndOrOrNow) {
        int[] nums = new int[keyForTruesTo-keyForFalsesFrom];
        int[] indices = new int[nrow];
        for(int i=0; i<nrow; i++){
            indices[i] = i;
        }
        for(int i=keyForFalsesFrom; i<keyForTruesTo; i++){
            nums[i-keyForFalsesFrom] = i;
        }
        boolean[] value = ifContains(indices, nums, false);
        this.addCriterion(value, isAndOrOrNow);
    }

    ArrayList<String> getNames(){
        return this.names;
    }

    ArrayList<boolean[]> getValues(){
        return this.values;
    }

    ArrayList<String> getAndOrOr(){
        return this.andOrOr;
    }

    void clear(){
        this.names = null;
        this.values = null;
        this.andOrOr = null;
    }

    void delAFilterKey(int which) {
        which = which < 0 ? this.values.size() + which : which;
        ArrayList<boolean[]> tempKeys = new ArrayList<boolean[]>();
        ArrayList<String> tempKeysStatus = new ArrayList<String>();
        ArrayList<String> tempAndOr = new ArrayList<String>();
        for(int i=0; i<which; i++){
            tempKeys.add(this.values.get(i));
            tempKeysStatus.add(this.names.get(i));
            tempAndOr.add(this.andOrOr.get(i));
        }
        for(int i=which+1; i<this.values.size()-1; i++){
            tempKeys.add(this.values.get(i));
            tempKeysStatus.add(this.names.get(i));
            tempAndOr.add(this.andOrOr.get(i));
        }
        this.values = (ArrayList<boolean[]>) tempKeys.clone();
        this.names = (ArrayList<String>) tempKeysStatus.clone();
        this.andOrOr = (ArrayList<String>) tempAndOr.clone();
//        this.setFinalResult();
        System.out.println("The specified FilterKeys has been deleted, and the FilterKey has been updated.");
    }

    /**
     * This method is used to deep copy the FilterKey object.
     *
     * @param old The FilterKey object to be copied.
     * @return The copied FilterKey object.
     */
    public FilterKey copy(FilterKey old){
        if(old.names!=null){
            this.names = new ArrayList<>();
            this.names.addAll(old.names);
        }
        if(old.values!=null){
            this.values = new ArrayList<>();
            for(boolean[] arr : old.values){
                boolean[] newArr = new boolean[arr.length];
                System.arraycopy(arr, 0, newArr, 0, arr.length);
                this.values.add(newArr);
            }
        }
        if(old.andOrOr!=null) {
            this.andOrOr = new ArrayList<>();
            this.andOrOr.addAll(old.andOrOr);
        }
        return this;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterKey filterKey = (FilterKey) o;
        return Objects.equals(names, filterKey.names) && Objects.equals(values, filterKey.values) && Objects.equals(andOrOr, filterKey.andOrOr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(names, values, andOrOr);
    }
}
