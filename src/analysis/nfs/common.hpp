/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/
#ifndef __NFSDSANALYSIS_MOD_H
#define __NFSDSANALYSIS_MOD_H

#include <string>

#include <Lintel/ConstantString.hpp>
#include <Lintel/HashTable.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

class NFSDSModule : public DataSeriesModule {
public:
    virtual ~NFSDSModule();
    virtual void printResult() = 0;
};

// TODO: move these into Lintel
std::string maybehexstring(const ConstantString &in);
std::string hexstring(const ConstantString &in);

// Note that the same filehandle is used by multiple servers (the
// caches), and so the server that is listed in the data is just one
// of the servers that had this filehandle.

struct fh2mountData {
    static void pruneToMountPart(std::string &adjust);
    static std::string pruneToMountPart(const ConstantString &from);
    static bool equalMountParts(const std::string &v1, const std::string &v2);
    // the mountpartfh is the chunk which is just the part of the
    // filehandle that is consistent for all the files on the fh.
    ConstantString mountpartfh, fullfh, pathname;
    ExtentType::int32 server;
    int common_bytes_seen_count;
    fh2mountData(const ExtentType::byte *a, int b) {
	std::string tmp(reinterpret_cast<const char *>(a),b);
	pruneToMountPart(tmp);
	mountpartfh = tmp;
    }
	
    fh2mountData(const ConstantString &a) {
	mountpartfh = pruneToMountPart(a);
    }

    fh2mountData(const ConstantString &a, const ConstantString &b, ExtentType::int32 c) 
	: fullfh(a), pathname(b), server(c), common_bytes_seen_count(0) {
	mountpartfh = pruneToMountPart(fullfh);
    }
};

class fh2mountHash {
public:
    unsigned int operator()(const fh2mountData &k) const {
	return lintel::hashBytes(k.mountpartfh.data(),k.mountpartfh.size());
    }
};

class fh2mountEqual {
public:
    bool operator()(const fh2mountData &a, const fh2mountData &b) const {
	return a.mountpartfh == b.mountpartfh;
    }
};

typedef HashTable<fh2mountData, fh2mountHash, fh2mountEqual> fh2mountT;
extern fh2mountT fh2mount;

namespace NFSDSAnalysisMod {
    // if the number of mount points hits this value, then we wil stop adding to the table
    // TODO: add a function to get the fh2mount information that can return an error if the 
    // contant was exceeded.
    const uint32_t max_mount_points_expected = 100000; 
    NFSDSModule *newFillFH2FN_HashTable(DataSeriesModule &source);
    NFSDSModule *newFillMount_HashTable(DataSeriesModule &source);
};

std::string *fnByFileHandle(const ConstantString &fh);
inline std::string *fnByFileHandle(const std::string &fh) {
    return fnByFileHandle(ConstantString(fh));
}

uint8_t opIdToUnifiedId(uint8_t nfs_version, uint8_t op_id);
const std::string &unifiedIdToName(uint8_t unified_id);
uint8_t nameToUnifiedId(const std::string &name);
bool validateUnifiedId(uint8_t nfs_version, uint8_t op_id, 
		       const std::string &op_name);
unsigned getMaxUnifiedId();

uint64_t md5FileHash(const Variable32Field &filehandle);

double doubleModArg(const std::string &optname, const std::string &arg);
#endif
