package ToolWorkSpace.SQL_resolver.POJO;

import java.util.List;

public class LinkTable {
    private TablePOJO linkTablePOJO;
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
