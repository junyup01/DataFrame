package com.jvcats.dataframe.base;

import java.util.Objects;

public class GroupKey {
    private String name;
    private String value;
    private String valueType;

    GroupKey(){}

    GroupKey(String name, String value) {
        this.name = name;
        this.value = value;
    }

    String getName() {
        return name;
    }

    String getValue() {
        return value;
    }

    String getValueType() {
        return valueType;
    }

    void setValueType(String valueType) {
        this.valueType = valueType;
    }

    void setName(String name) {
        this.name = name;
    }

    void setValue(String value) {
        this.value = value;
    }

    void clear() {
        name = null;
        value = null;
        valueType = null;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return Objects.equals(name, groupKey.name) && Objects.equals(value, groupKey.value) && Objects.equals(valueType, groupKey.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, valueType);
    }
}
