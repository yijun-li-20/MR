package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/21.
 */
public class WordCount {

    public static List<KeyValue> mapFunc(String file, String value) {
        /*
            words := strings.FieldsFunc(contents, func(r rune) bool {
                return !unicode.IsLetter(r)
            })
            var kvs []mapreduce.KeyValue
            for _, word := range words {
                kvs = append(kvs, mapreduce.KeyValue{word, "1"})
            }
            return kvs
         */
        List<KeyValue> list = new ArrayList<>();
        Pattern p = Pattern.compile("[a-zA-Z0-9]+");
        Matcher m = p.matcher(value);
        while (m.find()) {
            list.add(new KeyValue(m.group(), "1"));
        }
        return list;
    }

    public static String reduceFunc(String key, String[] values) {
        /*
                return strconv.Itoa(len(values))
         */
        int count = 0;
        for (String item : values) {
            count += Integer.valueOf(item);
        }
        return Integer.toString(count);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("error: see usage comments in file");
        } else if (args[0].equals("master")) {
            Master mr;

            String src = args[2];
            File file = new File(".");
            String[] files = file.list(new WildcardFileFilter(src));
            if (args[1].equals("sequential")) {
                mr = Master.sequential("wcseq", files, 3, WordCount::mapFunc, WordCount::reduceFunc);
            } else {
                mr = Master.distributed("wcseq", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], WordCount::mapFunc, WordCount::reduceFunc, 100, null);
        }
    }
}
