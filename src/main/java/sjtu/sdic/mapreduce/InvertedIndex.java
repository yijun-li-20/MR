package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/24.
 */
public class InvertedIndex {

    public static List<KeyValue> mapFunc(String file, String value) {
        /**
         *  words := strings.FieldsFunc(value, func(r rune) bool {
         *         return !unicode.IsLetter(r)
         *     })
         *     var kvs []mapreduce.KeyValue
         *     for _, word := range words {
         *         kvs = append(kvs, mapreduce.KeyValue{word, document})
         *     }
         *     return kvs
         */
        List<KeyValue> list = new ArrayList<>();
        Pattern p = Pattern.compile("[a-zA-Z]+");
        Matcher m = p.matcher(value);
        while (m.find()) {
            list.add(new KeyValue(m.group(), file));
        }
        return list;
    }

    public static String reduceFunc(String key, String[] values) {
        /**
         * values = removeDuplicationAndSort(values)
         * return strconv.Itoa(len(values)) + " " + strings.Join(values, ",")
         * func removeDuplicationAndSort(values []string) []string {
         * kvs := make(map[string]struct{})
         *     for _, value := range values {
         *         _, ok := kvs[value]
         *         if !ok {
         *             kvs[value] = struct{}{}
         *         }
         *     }
         *     var ret []string
         *     for k := range kvs {
         *         ret = append(ret, k)
         *     }
         *     sort.Strings(ret)
         *     return ret
         * }
         */
        //Arrays.sort(values);
        Set<String> ts = new TreeSet<>();
        Collections.addAll(ts, values);
        StringBuffer s = new StringBuffer();
        for (String it : ts) {
            s.append(" ");
            s.append(it);

        }

        return ts.size() + s.toString();
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
                mr = Master.sequential("iiseq", files, 3, InvertedIndex::mapFunc, InvertedIndex::reduceFunc);
            } else {
                mr = Master.distributed("iiseq", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], InvertedIndex::mapFunc, InvertedIndex::reduceFunc, 100, null);
        }
    }
}
