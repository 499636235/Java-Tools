package ToolWorkSpace.SQL_resolver.POJO;

import java.util.ArrayList;
import java.util.List;

/**
 * 双表关联对象
 */
public class LinkPOJO {

    //表1
    private TablePOJO table1 = null;

    //表2
    private TablePOJO table2 = null;

    //关联条件
    private List<String> linkConditionList = new ArrayList<>();


    public TablePOJO getOtherTablePOJO(TablePOJO tablePOJO) {
        if (table1.getTableId().equals(tablePOJO.getTableId())) {
            return table2;
        } else if (table2.getTableId().equals(tablePOJO.getTableId())) {
            return table1;
        }
        return null;
    }

    public LinkPOJO(TablePOJO table1, TablePOJO table2, List<String> linkConditionList) {
        this.table1 = table1;
        this.table2 = table2;
        this.linkConditionList = linkConditionList;
        table1.addLinkPOJOList(this);
        table2.addLinkPOJOList(this);
    }

    @Override
    public String toString() {
        return "LinkPOJO{" +
                "table1=" + table1.getTableName() +
                ", table2=" + table2.getTableName() +
                ", linkConditionList=" + linkConditionList +
                '}';
    }
}
