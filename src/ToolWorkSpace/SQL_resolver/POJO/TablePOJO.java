package ToolWorkSpace.SQL_resolver.POJO;

import java.util.ArrayList;
import java.util.List;


/**
 * 单表对象
 */
public class TablePOJO {

    //表ID
    private Integer tableId = null;

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
     * 为了好看而已
     *
     * @param tablePOJO
     * @return
     */
    public LinkPOJO getLinkPOJOWith(TablePOJO tablePOJO) {
        return getLinkPOJOWith(tablePOJO, null);
    }

    /**
     * (方案2备份)
     * 获取与另一张表 tablePOJO 的 双表关联对象 linkPOJO
     *
     * @param tablePOJO
     * @param linkHistory
     * @return
     */
    public LinkPOJO getLinkPOJOWith(TablePOJO tablePOJO, List<Integer> linkHistory) {
        LinkPOJO resultLinkPOJO = null;

        // 关联轨迹
        if (linkHistory == null) {
            // 如果为空则初始化
            linkHistory = new ArrayList<>();
        }

        //将当前递归的对象记录到关联轨迹中
        linkHistory.add(tablePOJO.getTableId());

        //遍历表 tablePOJO 的所有关联表
        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            //如果其中的一张关联表是 本表
            if (linkPOJO.getOtherTablePOJO(tablePOJO).getTableId().equals(this.tableId)) {
                //返回 双表关联对象 linkPOJO
                return linkPOJO;
            }
        }

        //遍历所有关联表后没有找到本表，则递归遍历关联表的关联表
        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            //如果要递归的对象已经被递归过，则为了防止循环递归 直接跳过
            if (linkHistory.contains(linkPOJO.getOtherTablePOJO(tablePOJO).getTableId())) {
                continue;
            }

            //带上关联轨迹进入递归(防止迷路)
            resultLinkPOJO = getLinkPOJOWith(linkPOJO.getOtherTablePOJO(tablePOJO), linkHistory);
            //递归结束后如果有返回值则就是要找的对象
            if (resultLinkPOJO != null) {
                return resultLinkPOJO;
            }
        }
        return null;
    }



    /**
     * 为了好看而已
     *
     * @param tablePOJO
     * @return
     */
    public List<LinkPOJO> getLinkPOJOListTo(TablePOJO tablePOJO) {
        return getLinkPOJOListTo(tablePOJO, null,null);
    }


    /**
     * (方案2 测试)
     * 获取与另一张表 tablePOJO 的 双表关联对象 linkPOJOList
     *
     * @param tablePOJO
     * @param linkHistory
     * @return
     */
    public List<LinkPOJO> getLinkPOJOListTo(TablePOJO tablePOJO, List<Integer> linkHistory, List<LinkPOJO> linkPOJOList) {

        // 双表关联对象 List
        if (linkPOJOList == null) {
            // 如果为空则初始化
            linkPOJOList = new ArrayList<>();
        }

        // 双表关联对象 List 的 长度
        int linkNum = linkPOJOList.size();

        // 关联轨迹
        if (linkHistory == null) {
            // 如果为空则初始化
            linkHistory = new ArrayList<>();
        }


        //将当前递归的对象记录到关联轨迹中(防止迷路)
        linkHistory.add(tablePOJO.getTableId());

        //遍历表 tablePOJO 的所有关联表
        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            //递归中止处
            //如果其中的一张关联表是 本表
            if (linkPOJO.getOtherTablePOJO(tablePOJO).getTableId().equals(this.tableId)) {
                //把这个 linkPOJO 加入 双表关联对象 List 中
                linkPOJOList.add(linkPOJO);
                //返回 双表关联对象 linkPOJO
                return linkPOJOList;
            }
        }

        //遍历所有关联表后没有找到本表，则递归遍历关联表的关联表
        for (LinkPOJO linkPOJO : tablePOJO.getLinkPOJOList()) {
            //如果要递归的对象已经被递归过，则为了防止循环递归 直接跳过
            if (linkHistory.contains(linkPOJO.getOtherTablePOJO(tablePOJO).getTableId())) {
                continue;
            }

            //带上关联轨迹进入递归(防止迷路)
            linkPOJOList = getLinkPOJOListTo(linkPOJO.getOtherTablePOJO(tablePOJO), linkHistory, linkPOJOList);
            //递归结束后如果List有增加新的linkPOJO，则说明末端已经关联到主表(递归中止了，已经开始回调)
            if (linkPOJOList.size() > linkNum) {
                //把这个 linkPOJO 加入 双表关联对象 List 中，记录整个关联轨迹
                linkPOJOList.add(linkPOJO);
                return linkPOJOList;
            }
        }
        return null;
    }


    public void addLinkPOJOList(LinkPOJO linkPOJO) {
        this.linkPOJOList.add(linkPOJO);
    }

    public TablePOJO(Integer tableId, String tableName) {
        this.tableId = tableId;
        this.tableName = tableName;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
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
