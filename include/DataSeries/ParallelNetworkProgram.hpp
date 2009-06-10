// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A class for creating DataSeries-based programs that run across multiple
    machines in parallel.
*/

#ifndef __DATASERIES_PARALLELNETWORKPROGRAM_H
#define __DATASERIES_PARALLELNETWORKPROGRAM_H

#include <vector>

#include <DataSeries/Extent.hpp>
#include <DataSeries/TypeIndexModule.hpp>

namespace dataseries {
/** An abstract class for implementing a DataSeries-based program that runs in parallel on
    multiple machines. In the first phase, the subclass' processExtentFromFile is called
    for each extent in the local DataSeries file. That method must return an extent, which
    is then partitioned (according to the user-supplied partitioner). Each partition is buffered
    and sent to a different node.
    In the second phase, the program receives records/extents over the network from all the
    other nodes, and processExtentFromNetwork is called. This function decides what to do with
    the data, but it does not return anything.
    Note that the subclass can also implement finishedFile and/or finishedNetwork to receive
    notifications when all of the local data has been read and all of the data has been received
    from the other nodes, respectively.
    Also note that a node must have local data in order to participate in the parallel
    computation. The justification for this limitation is that the network protocol currently
    does not convey extent type information, so the nodes are expected to learn about types
    from their local DataSeries files. */
template <typename F, typename P>
class ParallelNetworkProgram {
public:
    ParallelNetworkProgram(const std::vector<std::string> &node_names,
                           uint32_t node_index,
                           const std::string &input_file_prefix,
                           const std::string &extent_type_name,
                           const P &record_partitioner)
        : node_names(node_names), node_index(node_index),
          input_file_prefix(input_file_prefix), extent_type_name(extent_type_name),
          record_partitioner(record_partitioner) {}
    virtual ~ParallelNetworkProgram() {}

    virtual Extent* processExtentFromFile(Extent *extent) = 0;
    virtual void processExtentFromNetwork(Extent *extent, uint32_t source_node_index) = 0;

    virtual void finishedFile() {}
    virtual void finishedNetwork() {}

    void start();

private:
    std::vector<std::string> node_names;
    uint32_t node_index;
    std::string input_file_prefix;
    std::string extent_type_name;
    P record_partitioner;

    void startFile();
    void startNetwork();
};

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::start() {
    startFile();
    startNetwork();
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::startNetwork() {

    finishedNetwork();
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::startFile() {
    TypeIndexModule input_module(extent_type_name);
    input_module.addSource((boost::format("%s.%s") % input_file_prefix % node_index).str());
    Extent *extent = NULL;

    while ((extent = input_module.getExtent()) != NULL) {
        processExtentFromFile(extent);
    }

    finishedFile();
}

}

#endif
