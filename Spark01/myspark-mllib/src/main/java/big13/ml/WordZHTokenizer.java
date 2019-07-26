package big13.ml;

import org.apache.spark.SparkConf;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;

import java.util.List;

public class WordZHTokenizer {
    public static void main(String[] args) {
        WordConfTools.set("stopwords.path","classpath:mystopwords.txt");
        // 更菜词典路径后重新加载词典
        DictionaryFactory.reload();
//        List<Word> words = WordSegmenter.seg("南京市长江大桥是最长的大桥");
        List<Word> words = WordSegmenter.segWithStopWords("南京市长江大桥是最长的大桥");
        for (Word w : words) {
            System.out.println(w);
        }
    }
}
