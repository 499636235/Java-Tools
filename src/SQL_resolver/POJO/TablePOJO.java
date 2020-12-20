package SQL_resolver.POJO;

import java.util.ArrayList;
import java.util.List;


/**
 * 单表对象
 */
public class TablePOJO {

    /**
     * 表ID
     */
    private Integer tableId = null;

    /**
     * 表名
     */
    private String tableName = null;

    /**
     * (方案1)关联表List
     */
    private List<JoinTable> joinTableList = new ArrayList<>();

    /**
     * (方案2)双表关联对象List
     */
    private List<JoinPOJO> joinPOJOList = new ArrayList<>();


    /**
     * (方案1)
     * 获取与另一张表 tablePOJO 的 关联条件
     * 算法应该基于网状递归
     *
     * @param tablePOJO
     * @return
     */
    public List<String> getJoinConditionWith(TablePOJO tablePOJO) {
        List<String> resultList = null;
        for (JoinTable joinTable : tablePOJO.getJoinTableList()) {
            if (joinTable.getJoinTablePOJO().getTableName().equals(this.tableName)) {
                return joinTable.getJoinCondition();
            }
        }

        for (JoinTable joinTable : tablePOJO.getJoinTableList()) {
            resultList = getJoinConditionWith(joinTable.getJoinTablePOJO());
            if (resultList != null) {
                return resultList;
            }
        }


        return resultList;
    }

    /**
     * 简化接口(方案2)
     *
     * @param tablePOJO
     * @return
     */
    public List<JoinPOJO> getJoinPOJOListTo(TablePOJO tablePOJO) {
        return getJoinPOJOListTo(tablePOJO, null, null);
    }

    /**
     * (方案2)
     * 获取与另一张表 tablePOJO 的 双表关联对象 joinPOJOList
     *
     * @param tablePOJO
     * @param joinHistory
     * @return
     */
    public List<JoinPOJO> getJoinPOJOListTo(TablePOJO tablePOJO, List<Integer> joinHistory, List<JoinPOJO> joinPOJOList) {

        // 双表关联对象 List
        if (joinPOJOList == null) {
            // 如果为空则初始化
            joinPOJOList = new ArrayList<>();
        }

        // 双表关联对象 List 的 长度
        int joinNum = joinPOJOList.size();

        // 关联轨迹
        if (joinHistory == null) {
            // 如果为空则初始化
            joinHistory = new ArrayList<>();
        }


        //将当前递归的对象记录到关联轨迹中(防止迷路)
        joinHistory.add(tablePOJO.getTableId());

        //遍历表 tablePOJO 的所有关联表
        for (JoinPOJO joinPOJO : tablePOJO.getJoinPOJOList()) {
            //递归中止处
            //如果其中的一张关联表是 本表
            if (joinPOJO.getOtherTablePOJO(tablePOJO).getTableId().equals(this.tableId)) {
                //把这个 joinPOJO 加入 双表关联对象 List 中
                joinPOJOList.add(joinPOJO);
                //返回 双表关联对象 joinPOJO
                return joinPOJOList;
            }
        }

        //遍历所有关联表后没有找到本表，则递归遍历关联表的关联表
        for (JoinPOJO joinPOJO : tablePOJO.getJoinPOJOList()) {
            //如果要递归的对象已经被递归过，则为了防止循环递归 直接跳过
            if (joinHistory.contains(joinPOJO.getOtherTablePOJO(tablePOJO).getTableId())) {
                continue;
            }

            //带上关联轨迹进入递归(防止迷路)
            joinPOJOList = getJoinPOJOListTo(joinPOJO.getOtherTablePOJO(tablePOJO), joinHistory, joinPOJOList);
            //递归结束后如果List有增加新的joinPOJO，则说明末端已经关联到主表(递归中止了，已经开始回调)
            if (joinPOJOList.size() > joinNum) {
                //把这个 joinPOJO 加入 双表关联对象 List 中，记录整个关联轨迹
                joinPOJOList.add(joinPOJO);
                return joinPOJOList;
            }
        }
        return null;
    }

    /**
     * 把 joinPOJO 加入该 TablePOJO 的 joinPOJOList 中
     *
     * @param joinPOJO
     */
    public void addJoinPOJOList(JoinPOJO joinPOJO) {
        this.joinPOJOList.add(joinPOJO);
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

    public List<JoinTable> getJoinTableList() {
        return joinTableList;
    }

    public void setJoinTableList(List<JoinTable> joinTableList) {
        this.joinTableList = joinTableList;
    }

    public List<JoinPOJO> getJoinPOJOList() {
        return joinPOJOList;
    }

    public void setJoinPOJOList(List<JoinPOJO> joinPOJOList) {
        this.joinPOJOList = joinPOJOList;
    }


}
