package ToolWorkSpace.SQL_resolver.POJO;

import java.util.List;

public class TablePOJO {
    private String tableName;
    private List<LinkTable> linkTableList;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<LinkTable> getLinkTableList() {
        return linkTableList;
    }

    public void setLinkTableList(List<LinkTable> linkTableList) {
        this.linkTableList = linkTableList;
    }


    public List<String> getLinkConditionWith(TablePOJO tablePOJO) {
        List<String> resultList = null;
        for (LinkTable linkTable : tablePOJO.getLinkTableList()) {
            if (linkTable.getLinkTablePOJO().getTableName().equals(this.tableName)) {
                return linkTable.getLinkCondition();
            }
        }

        for (LinkTable linkTable : tablePOJO.getLinkTableList()) {
            resultList = getLinkConditionWith(linkTable.getLinkTablePOJO());
            if(resultList!=null){
                return resultList;
            }
        }


        return resultList;
    }
}
