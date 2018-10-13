import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;


public class MerlinScheduler implements IScheduler{


    public void prepare(Map conf) {

    }


    public void schedule(Topologies topologies, Cluster cluster) {
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
        Map<Object, SupervisorDetails> supervisors = new HashMap<>();
        for(SupervisorDetails s : supervisorDetails){
            Map<String, Object> metadata = (Map<String, Object>)s.getSchedulerMeta();
            if(metadata.get("group-id") != null){
                System.out.println(metadata.get("group-id"));

                supervisors.put((String)metadata.get("group-id"), s);

            }
        }

        for(TopologyDetails t : topologyDetails){
            if(!cluster.needsScheduling(t)) continue;
            StormTopology topology = t.getTopology();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            JSONParser parser = new JSONParser();
            try{
                for(String name : bolts.keySet()){
                    Bolt bolt = bolts.get(name);
                    JSONObject conf = (JSONObject)parser.parse(bolt.get_common().get_json_conf());
                    if(conf.get("group-id") != null && supervisors.get(conf.get("group-id").toString()) != null){
                        Long gid = (Long)conf.get("group-id");
                        SupervisorDetails supervisor = supervisors.get(gid.toString());
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);
                        if(!availableSlots.isEmpty() && executors != null){
                            cluster.assign(availableSlots.get(0), t.getId(), executors);
                        }
                    }
                }
                for(String name : spouts.keySet()){
                    SpoutSpec spout = spouts.get(name);
                    JSONObject conf = (JSONObject)parser.parse(spout.get_common().get_json_conf());
                    if(conf.get("group-id") != null && supervisors.get(conf.get("group-id").toString()) != null){
                        Long gid = (Long)conf.get("group-id");
                        SupervisorDetails supervisor = supervisors.get(gid.toString());
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);
                        if(!availableSlots.isEmpty() && executors != null){
                            cluster.assign(availableSlots.get(0), t.getId(), executors);
                        }
                    }
                }
            }catch(ParseException pe){
                pe.printStackTrace();
            }
        }
    }

}
