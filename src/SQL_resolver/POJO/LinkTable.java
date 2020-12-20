package SQL_resolver.POJO;

import java.util.List;

/**
 * 关联表对象
 */
public class LinkTable {

    private TablePOJO linkTablePOJO;

    //关联条件(对象类 未定,暂时为List<String>)
    private List<String> linkCondition;

    public TablePOJO getLinkTablePOJO() {
        return linkTablePOJO;
    }

    public void setLinkTablePOJO(TablePOJO linkTablePOJO) {
        this.linkTablePOJO = linkTablePOJO;
    }

    public List<String> getLinkCondition() {
        return linkCondition;
    }

    public void setLinkCondition(List<String> linkCondition) {
        this.linkCondition = linkCondition;
    }
}
