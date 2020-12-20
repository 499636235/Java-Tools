package SQL_resolver.POJO;

import java.util.List;

/**
 * 关联表对象
 */
public class JoinTable {

    private TablePOJO joinTablePOJO;

    //关联条件(对象类 未定,暂时为List<String>)
    private List<String> joinCondition;

    public TablePOJO getJoinTablePOJO() {
        return joinTablePOJO;
    }

    public void setJoinTablePOJO(TablePOJO joinTablePOJO) {
        this.joinTablePOJO = joinTablePOJO;
    }

    public List<String> getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(List<String> joinCondition) {
        this.joinCondition = joinCondition;
    }
}
