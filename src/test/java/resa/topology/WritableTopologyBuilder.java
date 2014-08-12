package resa.topology;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import resa.migrate.HdfsWritableBolt;

/**
 * Created by ding on 14-4-26.
 */
public class WritableTopologyBuilder extends ResaTopologyBuilder {

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
        bolt = new HdfsWritableBolt(bolt);
        return super.setBolt(id, bolt, parallelismHint);
    }

}
