package io.datadynamics.nifi.db;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutingResult {

    private final Map<Relationship, List<FlowFile>> routedFlowFiles = new HashMap<>();

    public void routeTo(final FlowFile flowFile, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).add(flowFile);
    }

    public void routeTo(final List<FlowFile> flowFiles, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).addAll(flowFiles);
    }

    public void merge(final RoutingResult r) {
        r.getRoutedFlowFiles().forEach((relationship, routedFlowFiles) -> routeTo(routedFlowFiles, relationship));
    }

    public Map<Relationship, List<FlowFile>> getRoutedFlowFiles() {
        return routedFlowFiles;
    }

    public boolean contains(Relationship relationship) {
        return routedFlowFiles.containsKey(relationship) && !routedFlowFiles.get(relationship).isEmpty();
    }
}