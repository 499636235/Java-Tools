import javax.swing.filechooser.FileSystemView;
import java.io.File;

public class test {
    public static void main(String[] args) {
        String s = "D:\\zkr\\项目\\大数据\\doc\\06.项目开发\\01.数据平台\\ETL脚本\\建表脚本\\ods\\reldata\\cmsp\\dt\\";
        File inputFilePath = new File(s);
        for (File inputFile : inputFilePath.listFiles()){
            System.out.println(inputFile.getName());
        }
    }
}
