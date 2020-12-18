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
        if (table1 == tablePOJO){
            return table2;
        }else if (table2 == tablePOJO) {
            return table1;
        }
        return null;
    }
}
