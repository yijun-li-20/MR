package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static sjtu.sdic.mapreduce.common.Utils.debugEnabled;
import static sjtu.sdic.mapreduce.common.Utils.reduceName;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * doReduce manages one reduce task: it should read the intermediate
     * files for the task, sort the intermediate key/value pairs by key,
     * call the user-defined reduce function {@code reduceFunc} for each key,
     * and write reduceFunc's output to disk.
     * <p>
     * You'll need to read one intermediate file from each map task;
     * {@code reduceName(jobName, m, reduceTask)} yields the file
     * name from map task m.
     * <p>
     * Your {@code doMap()} encoded the key/value pairs in the intermediate
     * files, so you will need to decode them. If you used JSON, you can refer
     * to related docs to know how to decode.
     * <p>
     * In the original paper, sorting is optional but helpful. Here you are
     * also required to do sorting. Lib is allowed.
     * <p>
     * {@code reduceFunc()} is the application's reduce function. You should
     * call it once per distinct key, with a slice of all the values
     * for that key. {@code reduceFunc()} returns the reduced value for that
     * key.
     * <p>
     * You should write the reduce output as JSON encoded KeyValue
     * objects to the file named outFile. We require you to use JSON
     * because that is what the merger than combines the output
     * from all the reduce tasks expects. There is nothing special about
     * JSON -- it is just the marshalling format we chose to use.
     * <p>
     * Your code here (Part I).
     *
     * @param jobName    the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile    write the output here
     * @param nMap       the number of map tasks that were run ("M" in the paper)
     * @param reduceFunc user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceFunc) {
        List<String> keys = new ArrayList<>();
        Map<String, List<String>> kvs = new TreeMap<>();
        for (int mapIndex = 0; mapIndex < nMap; mapIndex++) {
            /*
                midDatafileName := reduceName(jobName, mapTaskNumber, reduceTask)
                file, err := os.Open(midDatafileName)
                if err != nil {
                    panic(err)
                }
                defer file.Close()
             */
            File file = new File(reduceName(jobName, mapIndex, reduceTask));
            FileReader fr = null;
            try {
                fr = new FileReader(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            char[] content = new char[Integer.parseInt(String.valueOf(file.length()))];
            try {
                fr.read(content);
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (debugEnabled) System.out.println(content);
            /*
                dec := json.NewDecoder(file)
             */
            JSONArray fileArray = JSONArray.parseArray(String.valueOf(content));
            List<KeyValue> list = JSONObject.parseArray(fileArray.toJSONString(), KeyValue.class);

            /*
                for {
                    var kv KeyValue
                    err = dec.Decode(&kv)
                    if err != nil {
                        break
                    }
                    values, ok := kvs[kv.Key]
                    if ok {
                        kvs[kv.Key] = append(values, kv.Value)
                    } else {
                        kvs[kv.Key] = []string{kv.Value}
                    }
                }
             */
            for (KeyValue item : list) {
                if (!kvs.containsKey(item.key)) {
                    keys.add(item.key);
                    List<String> pair = new ArrayList<>();
                    pair.add(item.value);
                    kvs.put(item.key, pair);
                } else {
                    List<String> values = kvs.get(item.key);
                    values.add(item.value);
                    kvs.put(item.key, values);
                }
            }
        }
        /*
            outputFile, err := os.Create(outFile)
            if err != nil {
                panic(err)
            }
            defer outputFile.Close()
            enc := json.NewEncoder(outputFile)
            for key, values := range kvs {
                enc.Encode(KeyValue{key, reduceF(key, values)})
            }
         */

        try {
            FileWriter fw = new FileWriter(outFile);
            JSONObject jsonObject = new JSONObject();
            for (String key : keys) {
                List<String> l = kvs.get(key);
                String[] values = l.toArray(new String[l.size()]);
                String value = reduceFunc.reduce(key, values);
                jsonObject.put(key, value);
            }
            fw.write(jsonObject.toJSONString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
