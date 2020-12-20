package SQL_resolver.POJO;

import java.util.ArrayList;
import java.util.List;

/**
 * 双表关联对象
 */
public class JoinPOJO {

    /**
     * 表1
     */
    private TablePOJO table1 = null;

    /**
     * 表2
     */
    private TablePOJO table2 = null;

    /**
     * 关联条件
     */
    private List<String> joinConditionList = new ArrayList<>();

    /**
     * 获取 tablePOJO 在该 JoinPOJO 中关联的另一个 TablePOJO 对象
     *
     * @param tablePOJO
     * @return TablePOJO
     */
    public TablePOJO getOtherTablePOJO(TablePOJO tablePOJO) {
        if (table1.getTableId().equals(tablePOJO.getTableId())) {
            return table2;
        } else if (table2.getTableId().equals(tablePOJO.getTableId())) {
            return table1;
        }
        return null;
    }

    /**
     * 除了充当构造器的功能，还把该 JoinPOJO 加入了 这两个 tablePOJO 的 joinPOJOList 中
     *
     * @param table1
     * @param table2
     * @param joinConditionList
     */
    public JoinPOJO(TablePOJO table1, TablePOJO table2, List<String> joinConditionList) {
        this.table1 = table1;
        this.table2 = table2;
        this.joinConditionList = joinConditionList;
        table1.addJoinPOJOList(this);
        table2.addJoinPOJOList(this);
    }

    /**
     * 输出调试用
     *
     * @return
     */
    @Override
    public String toString() {
        return "JoinPOJO{" +
                "table1=" + table1.getTableName() +
                ", table2=" + table2.getTableName() +
                ", joinConditionList=" + joinConditionList +
                '}';
    }
}
