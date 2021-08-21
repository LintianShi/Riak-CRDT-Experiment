package Tools;

import java.io.File;
import java.util.ArrayList;

public class FileAnalyzer {

    public static void getFiles(ArrayList<File> fileList, String path) {
        File[] allFiles = new File(path).listFiles();
        for (int i = 0; i < allFiles.length; i++) {
            File file = allFiles[i];

            if (file.isFile()) {
                fileList.add(file);
            } else  {
                getFiles(fileList, file.getAbsolutePath());
            }
        }
    }



}
