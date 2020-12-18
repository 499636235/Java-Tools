package ToolWorkSpace.SQL_resolver.POJO;

import java.util.ArrayList;
import java.util.List;


/**
 * 单表对象
 */
public class TablePOJO {

    //表名
    private String tableName = null;

    //(方案1)关联表List
    private List<LinkTable> linkTableList = new ArrayList<>();


    //(方案2)双表关联对象List
    private List<LinkPOJO> linkPOJOList = new ArrayList<>();


    /**
     * (方案1)
     * 获取与另一张表 tablePOJO 的 关联条件
     * 算法应该基于网状递归
     *
     * @param tablePOJO
     * @return
     */
    public List<String> getLinkConditionWith(TablePOJO tablePOJO) {
        List<String> resultList = null;
        for (LinkTable linkTable : tablePOJO.getLinkTableList()) {
            if (linkTable.getLinkTablePOJO().getTableName().equals(this.tableName)) {
                return linkTable.getLinkCondition();
            }
        }

        for (LinkTable linkTable : tablePOJO.getLinkTableList()) {
            resultList = getLinkConditionWith(linkTable.getLinkTablePOJO());
            if (resultList != null) {
                return resultList;
            }
        }


        return resultList;
    }

    /**
     * (方案2)
     * 获取与另一张表 tablePOJO 的 双表关联对象
     *
     * @param tablePOJO
     * @return
     */
    public LinkPOJO getLinkPOJOWith(TablePOJO tablePOJO) {
        LinkPOJO resultList = null;
        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            //这个地方得改！如果出现另一张同名表就不对了！
            if (linkPOJO.getOtherTablePOJO(tablePOJO).getTableName().equals(this.tableName)) {
                return linkPOJO;
            }
        }

        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            resultList = getLinkPOJOWith(linkPOJO.getOtherTablePOJO(tablePOJO));
            if (resultList != null) {
                return resultList;
            }
        }
        return null;
    }

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

    public List<LinkPOJO> getLinkPOJOList() {
        return linkPOJOList;
    }

    public void setLinkPOJOList(List<LinkPOJO> linkPOJOList) {
        this.linkPOJOList = linkPOJOList;
    }
}
