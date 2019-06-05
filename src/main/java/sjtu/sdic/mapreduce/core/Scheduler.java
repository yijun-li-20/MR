package sjtu.sdic.mapreduce.core;

import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.exception.SofaTimeOutException;
import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;
import sjtu.sdic.mapreduce.rpc.WorkerRpcService;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Cachhe on 2019/4/22.
 */
public class Scheduler {

    /**
     * schedule() starts and waits for all tasks in the given phase (mapPhase
     * or reducePhase). the mapFiles argument holds the names of the files that
     * are the inputs to the map phase, one per map task. nReduce is the
     * number of reduce tasks. the registerChan argument yields a stream
     * of registered workers; each item is the worker's RPC address,
     * suitable for passing to {@link Call}. registerChan will yield all
     * existing registered workers (if any) and new ones as they register.
     *
     * @param jobName job name
     * @param mapFiles files' name (if in same dir, it's also the files' path)
     * @param nReduce the number of reduce task that will be run ("R" in the paper)
     * @param phase MAP or REDUCE
     * @param registerChan register info channel
     */
    public static void schedule(String jobName, String[] mapFiles, int nReduce, JobPhase phase, Channel<String> registerChan) {
        int nTasks = -1; // number of map or reduce tasks
        int nOther = -1; // number of inputs (for reduce) or outputs (for map)
        switch (phase) {
            case MAP_PHASE:
                nTasks = mapFiles.length;
                nOther = nReduce;
                break;
            case REDUCE_PHASE:
                nTasks = nReduce;
                nOther = mapFiles.length;
                break;
        }

        System.out.println(String.format("Schedule: %d %s tasks (%d I/Os)", nTasks, phase, nOther));

        //TODO:
        // All ntasks tasks have to be scheduled on workers. Once all tasks
        // have completed successfully, schedule() should return.
        // Your code here (Part III, Part IV).
        /**
         *     taskChan := make(chan int)
         *     var wg sync.WaitGroup
         *     go func() {
         *         for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
         *             taskChan <- taskNumber
         *             fmt.Printf("taskChan <- %d in %s\n", taskNumber, phase)
         *             wg.Add(1)
         *
         *         }
         *
         *         wg.Wait()                           //ntasks个任务执行完毕后才能通过
         *         close(taskChan)
         *     }()
         */
        CountDownLatch latch = new CountDownLatch(nTasks);
        Channel<Integer> taskChan = new Channel<Integer>();
        //Set hs = new HashSet();
        for(int taskNum=0;taskNum<nTasks;taskNum++) {
            final int i = taskNum;
            final int j = nOther;
            /*try {
                taskChan.write(taskNum);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        //}
        //for() {
                new Thread(()->{

                        String w = null;
                        try {
                            w = registerChan.read();
                            DoTaskArgs arg = new DoTaskArgs(jobName, mapFiles[i], phase, i, j);
                            try {
                                Call.getWorkerRpcService(w).doTask(arg);
                                registerChan.write(w);  // when success put worker back to waiting pool
                                latch.countDown();
                            }catch(SofaTimeOutException e) {
                                //hs.add(i);
                                w = registerChan.read();
                                Call.getWorkerRpcService(w).doTask(arg);
                                registerChan.write(w);  // when success put worker back to waiting pool
                                latch.countDown();
                            }


                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                }).start();
        }
   //     Iterator it = hs.iterator();
 /*       while(it.hasNext()){
            int item = (Integer) it.next();
            final int j = nOther;
            new Thread(()->{

                String w = null;
                try {
                    w = registerChan.read();
                    DoTaskArgs arg = new DoTaskArgs(jobName, mapFiles[item], phase, item, j);
                    try {
                        Call.getWorkerRpcService(w).doTask(arg);
                        registerChan.write(w);  // when success put worker back to waiting pool
                        latch.countDown();
                        it.remove();
                    }catch(SofaTimeOutException e) {
                        e.printStackTrace();
                    }


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }).start();
        }*/
             try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


         /*     for task := range taskChan {            //所有任务都处理完后跳出循环
         *         worker := <- registerChan         //消费worker
         *         fmt.Printf("given task %d to %s in %s\n", task, worker, phase)
         *
         *         var arg DoTaskArgs
         *         arg.JobName = jobName
         *         arg.Phase = phase
         *         arg.TaskNumber = task
         *         arg.NumOtherPhase = n_other
         *
         *         if phase == mapPhase {
         *             arg.File = mapFiles[task]
         *         }
         *
         *         go func(worker string, arg DoTaskArgs) {
         *             if call(worker, "Worker.DoTask", arg, nil) {
         *                 //执行成功后，worker需要执行其它任务
         *                 //注意：需要先掉wg.Done()，然后调register<-worker，否则会出现死锁
         *                 //fmt.Printf("worker %s run task %d success in phase %s\n", worker, task, phase)
         *                 wg.Done()
         *                 registerChan <- worker  //回收worker
         *             } else {
         *                 //如果失败了，该任务需要被重新执行
         *                 //注意：这里不能用taskChan <- task，因为task这个变量在别的地方可能会被修改。比如task 0执行失败了，我们这里希望
         *                 //将task 0重新加入到taskChan中，但是因为执行for循环的那个goroutine，可能已经修改task这个变量为1了，我们错误地
         *                 //把task 1重新执行了一遍，并且task 0没有得到执行。
         *                 taskChan <- arg.TaskNumber
         *             }
         *         }(worker, arg)
         *
         *     }
         */
        

        System.out.println(String.format("Schedule: %s done", phase));
    }
}
