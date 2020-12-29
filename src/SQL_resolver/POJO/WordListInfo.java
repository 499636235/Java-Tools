package SQL_resolver.POJO;

import java.util.List;

public class WordListInfo {
    /**
     * wordList 的唯一标识符
     */
    Integer wordListId;
    /**
     * 当前 wordList
     */
    List<String> wordList;
    /**
     * 标志当前 wordList 是否为SQL查询
     */
    boolean ifSqlFlag = true;

    public Integer getWordListId() {
        return wordListId;
    }

    public void setWordListId(Integer wordListId) {
        this.wordListId = wordListId;
    }

    public List<String> getWordList() {
        return wordList;
    }

    public void setWordList(List<String> wordList) {
        this.wordList = wordList;
    }

    public boolean getIfSqlFlag() {
        return ifSqlFlag;
    }

    public void setIfSqlFlag(boolean ifSqlFlag) {
        this.ifSqlFlag = ifSqlFlag;
    }
}
