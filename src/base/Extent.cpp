// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Extent implementation
*/

#include <math.h>
#include <errno.h>
#include <stdio.h>
#if defined(__linux__)
#   include <malloc.h>
#endif

#include <iostream>

#include <boost/limits.hpp>

#if (_FILE_OFFSET_BITS == 64 && !defined(_LARGEFILE64_SOURCE)) || defined(__CYGWIN__)
#define pread64 pread
#endif

#ifndef DATASERIES_ENABLE_BZIP2
#define DATASERIES_ENABLE_BZIP2 0
#endif

#ifndef DATASERIES_ENABLE_LZO
#define DATASERIES_ENABLE_LZO 0
#endif

#if DATASERIES_ENABLE_BZIP2
#include <bzlib.h>
#endif
#if DATASERIES_ENABLE_LZO
#include <lzo1x.h>
#endif

#include <zlib.h>
extern "C" {
#include <lzf.h>
}

#include <Lintel/Clock.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PThread.hpp>
#include <Lintel/StringUtil.hpp>

#define DS_RAW_EXTENT_PTR_DEPRECATED /* allowed */

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/DataSeriesFile.hpp>

using namespace std;
using boost::format;

extern "C" {
    char *dataseriesVersion() {
	return (char *)DATASERIES_VERSION;
    }
}

static bool did_init_malloc_tuning = false;

void Extent::ByteArray::initMallocTuning() {
    if (did_init_malloc_tuning) 
	return;
    did_init_malloc_tuning = true;

#if defined(M_MMAP_THRESHOLD)
    mallopt(M_MMAP_THRESHOLD, 1024*1024+8192);
#endif
}

Extent::ByteArray::~ByteArray() {
    delete [] beginV;
}

void Extent::ByteArray::clear() {
    delete [] beginV;
    beginV = endV = maxV = NULL;
}

void Extent::ByteArray::reserve(size_t reserve_bytes) {
    if (reserve_bytes <= static_cast<size_t>(maxV - beginV)) {
	return; // have enough already;
    }
    if (!did_init_malloc_tuning) {
	initMallocTuning();
    }
    size_t oldsize = size();
    byte *newV = new byte [reserve_bytes];

    size_t expect_align = 8;
    if (reserve_bytes == 4) { expect_align = 4; }
    size_t actual_align = reinterpret_cast<size_t>(newV) % expect_align;

    INVARIANT(actual_align == 0,
	      boost::format("internal error, misaligned malloc(%d) return %d mod %d\n")
	      % reserve_bytes % actual_align % expect_align);
    memcpy(newV,beginV,oldsize);
    delete [] beginV;
    beginV = newV;
    endV = newV + oldsize;
    maxV = newV + reserve_bytes;    
}


void Extent::ByteArray::copyResize(size_t newsize, bool zero_it) {
    size_t oldsize = size();
    int target_max = newsize < 2*oldsize ? 2*oldsize : newsize;
    reserve(target_max);
    endV = beginV + newsize;

    if (zero_it) {
	memset(beginV + oldsize,0,newsize - oldsize);
    }
}


static bool did_checks_init = false;
static bool preuncompress_check = true;
static bool postuncompress_check = true;
static bool unpack_variable32_check = true;

void Extent::setReadChecksFromEnv(bool defval) {
    bool pre = defval, post = defval, var32 = defval;

    if (getenv("DATASERIES_READ_CHECKS") != NULL) {
	vector<string> checks;
	split(getenv("DATASERIES_READ_CHECKS"), ",", checks);
	for(vector<string>::iterator i = checks.begin(); 
	    i != checks.end(); ++i) {
	    if (*i == "preuncompress") {
		pre = true;
	    } else if (*i == "postuncompress") {
		post = true;
	    } else if (*i == "variable32") {
		var32 = true;
	    } else if (*i == "all") {
		pre = post = var32 = true;
	    } else if (*i == "none") {
		pre = post = var32 = false;
	    } else {
		FATAL_ERROR(boost::format("unrecognized extent check %s; expected {preuncompress,postuncompress,variable32}")
			    % *i);
	    }
	}
    }
    preuncompress_check = pre;
    postuncompress_check = post;
    unpack_variable32_check = var32;
    did_checks_init = true;
}

#if DATASERIES_ENABLE_LZO
static int lzo_init = 0;
#endif

const std::string Extent::in_memory_str("in-memory");

void Extent::init() {
#if DATASERIES_ENABLE_LZO
    if (lzo_init == 0) {
        static PThreadMutex mutex;

        PThreadScopedLock lock(mutex);
        if (lzo_init == 0) {
            INVARIANT(lzo_init() == LZO_E_OK, "lzo_init() failed ?!");
            lzo_init = 1;
        }
    }
#endif
    // the default offset for variable sized fields is pointed to
    // offset 0; so we set up the 0 entry in the variable sized data
    // to be an 0 sized variable bit.
    variabledata.resize(4);
    // slightly incestuous interaction between Variable32Field and
    // Extent, but probably ok.
    *(int32 *)variabledata.begin() = 0;
    extent_source = in_memory_str;
    extent_source_offset = -1;
}

Extent::Extent(const ExtentTypeLibrary &library, 
	       Extent::ByteArray &packeddata,
	       const bool need_bitflip)
    : type(*library.getTypeByName(getPackedExtentType(packeddata)))
{
    init();
    unpackData(packeddata, need_bitflip);
}

Extent::Extent(const ExtentType &_type,
	       Extent::ByteArray &packeddata,
	       const bool need_bitflip)
    : type(_type)
{
    init();
    unpackData(packeddata, need_bitflip);
}

Extent::Extent(const ExtentType &_type)
    : type(_type)
{
    init();
}

Extent::Extent(const string &xmltype)
    : type(ExtentTypeLibrary::sharedExtentType(xmltype))
{
    init();
}

namespace {
    const ExtentType &toReference(const ExtentType *from) {
        INVARIANT(from != NULL, "bad cast to reference");
        return *from;
    }
}

Extent::Extent(ExtentSeries &myseries)
    : type(toReference(myseries.getType()))
{
    init();
    if (myseries.extent() == NULL) {
	myseries.setExtent(*this);
    }
}

Extent::~Extent() {
    INVARIANT(extent_source_offset != -2, "Duplicate delete of extent");
        
    try {
        Ptr e = shared_from_this();
        FATAL_ERROR("Internal error, destructor called, but there is still a shared pointer to us");
    } catch (std::exception &) {
        // ok
    }
    extent_source_offset = -2;
}

void Extent::swap(Extent &with) { 
    INVARIANT(&with.type == &type, "can't swap between incompatible types");
    fixeddata.swap(with.fixeddata);
    variabledata.swap(with.variabledata);
}

void Extent::createRecords(unsigned int nrecords) {
    fixeddata.resize(fixeddata.size() + nrecords * type.rep.fixed_record_size);
}    

struct variableDuplicateEliminate {
    ExtentType::byte *varbits;
    variableDuplicateEliminate(ExtentType::byte *a) : varbits(a) {}
};

class variableDuplicateEliminate_Equal {
public:
    typedef ExtentType::int32 int32;
    bool operator()(const variableDuplicateEliminate &a, 
		    const variableDuplicateEliminate &b) const {
	int32 size_a = *(int32 *)(a.varbits);
	int32 size_b = *(int32 *)(b.varbits);
	if (size_a != size_b) return false;
	return memcmp(a.varbits,b.varbits,size_a + 4) == 0;
    }
};

class variableDuplicateEliminate_Hash {
public:
    typedef ExtentType::int32 int32;
    unsigned int operator()(const variableDuplicateEliminate &a) const {
	int32 size_a = *(int32 *)(a.varbits);
	return lintel::bobJenkinsHash(1776, a.varbits, 4+size_a);
    }
};

static bool compactIsNull(const ExtentType::byte *fixed_record, 
			  const ExtentType::nullCompactInfo &f) {
    DEBUG_INVARIANT(f.null_bitmask != 0 || f.null_offset == 0, "?");
    if (*(fixed_record + f.null_offset) & f.null_bitmask) {
	return true;
    } else {
	return false;
    }
}

static const bool debug_compact = false;
void Extent::compactNulls(Extent::ByteArray &fixed_coded) {
    if (debug_compact) {
	cout << format("compacting %s\n")
	    % hexstring(string((char *)fixed_coded.begin(), fixed_coded.size()));
    }
    INVARIANT(type.getPackNullCompact() == ExtentType::CompactNonBool, "bad");
    Extent::ByteArray into;
    into.resize(fixed_coded.size(), false); // no need to fill in

    // Dense packing the booleans is only really a win if we manage to
    // save a character; it's also going to cost a lot of cpu time, so
    // we choose to not do it.  I guess if we found something that had
    // lots of nullable booleans it could become worth it.

    byte *cur = into.begin();
    INVARIANT(type.rep.bool_bytes > 0, "?");
    for(byte *fixed_record = fixed_coded.begin();
	fixed_record != fixed_coded.end(); 
	fixed_record += type.rep.fixed_record_size) {

	if (debug_compact) {
	    cout << format("compact from@%d/%d row %d/%d to@%d\n")
		% (fixed_record - fixed_coded.begin()) 
		% (fixed_coded.size())
		% ((fixed_record - fixed_coded.begin())/type.rep.fixed_record_size)
		% (fixed_coded.size() / type.rep.fixed_record_size)
		% (cur - into.begin());
	}
	DEBUG_INVARIANT(static_cast<size_t>(cur - into.begin()) 
			<= static_cast<size_t>(fixed_coded.size()), 
			format("internal %d > %d") % (cur - into.begin()) 
			% fixed_coded.size());

	INVARIANT(cur + type.rep.fixed_record_size <= into.end(), "bad");
	memcpy(cur, fixed_record, type.rep.bool_bytes);
	cur += type.rep.bool_bytes;

	typedef vector<ExtentType::nullCompactInfo>::const_iterator nciiT;
	
	// copy the bytes...
	for(nciiT i = type.rep.nonbool_compact_info_size1.begin(); 
	    i != type.rep.nonbool_compact_info_size1.end(); ++i) {
	    DEBUG_INVARIANT(i->type == ExtentType::ft_byte, "bad");
	    if (compactIsNull(fixed_record, *i)) {
		DEBUG_INVARIANT(*(fixed_record + i->offset) == 0, "?");
		continue;
	    }
	    *cur = *(fixed_record + i->offset);
	    cur += 1;
	}
	// copy the 4 byte things
	for(nciiT i = type.rep.nonbool_compact_info_size4.begin(); 
	    i != type.rep.nonbool_compact_info_size4.end(); ++i) {
	    DEBUG_INVARIANT(i->type == ExtentType::ft_int32 ||
			    i->type == ExtentType::ft_variable32, "bad");
	    if (compactIsNull(fixed_record, *i)) {
		DEBUG_INVARIANT(*reinterpret_cast<int32_t *>(fixed_record + i->offset) == 0, "?");
		continue;
	    }
	    // pad to 4 byte boundary; do it here so we only pad if we 
	    // have to
	    for(; reinterpret_cast<size_t>(cur) % 4 != 0; ++cur) {
		*cur = '\0';
	    }
	    *reinterpret_cast<uint32_t *>(cur) = 
		*reinterpret_cast<uint32_t *>(fixed_record + i->offset);
	    cur += 4;
	}
	// copy the 8 byte things
	for(nciiT i = type.rep.nonbool_compact_info_size8.begin(); 
	    i != type.rep.nonbool_compact_info_size8.end(); ++i) {
	    DEBUG_INVARIANT(i->type == ExtentType::ft_int64 ||
			    i->type == ExtentType::ft_double, "bad");
	    if (compactIsNull(fixed_record, *i)) {
		DEBUG_INVARIANT(*reinterpret_cast<int64_t *>(fixed_record + i->offset) == 0, format("? %d") % ((fixed_record - fixed_coded.begin())/type.rep.fixed_record_size));
		continue;
	    }
	    // pad to 8 byte boundary, do it here so we only pad if needed
	    for(; reinterpret_cast<size_t>(cur) % 8 != 0; ++cur) {
		*reinterpret_cast<int32_t *>(cur) = 0;
	    }
	    *reinterpret_cast<uint64_t *>(cur) = 
		*reinterpret_cast<uint64_t *>(fixed_record + i->offset);
	    cur += 8;
	}
    }
    size_t new_size = cur - into.begin();
    INVARIANT(new_size <= into.size(), 
	      format("internal %d > %d") % new_size % into.size());
    if (debug_compact) {
	cout << format("compacted nulls %d -> %d\n")
	    % fixed_coded.size() % new_size;
	cout << format("compacted to %s\n")
	    % hexstring(string((char *)into.begin(), new_size));
    }
    into.resize(new_size);
    fixed_coded.swap(into);
}

#if defined(__i386__) || defined(__i486__)
#define HAVE_ASM_MEMCPY 

// from asm/string.h, explicitly marked as public domain.  Should
// re-test performance improvement; see below for x86_64 discussion.

static inline void * asm_memcpy(void * to, const void * from, size_t n) {
int d0, d1, d2;
__asm__ __volatile__(
	"rep ; movsl\n\t"
	"movl %4,%%ecx\n\t"
	"andl $3,%%ecx\n\t"
#if 1	/* want to pay 2 byte penalty for a chance to skip microcoded rep? */
	"jz 1f\n\t"
#endif
	"rep ; movsb\n\t"
	"1:"
	: "=&c" (d0), "=&D" (d1), "=&S" (d2)
	: "0" (n/4), "g" (n), "1" ((long) to), "2" ((long) from)
	: "memory");
return (to);
}
#endif

// For x86_64, linux memcpy from asm-x86_64/string.h may improve
// performance by 0.5% over glibc (RHEL4, 2x Dual Core Opteron 2216
// HE), but measurements are within stddev.  
// 
// small_memcpy_amd64 from
// http://www.mirror.inter.net.il/pub/NetBSD/NetBSD-current/xsrc/xorg/driver/xf86-video-sis/src/sis_memcpy.c
// with the rep movsq turned into just a rep movsl also seems to be a
// little better if slightly less than the linux memcpy.
//
// amd's recommended (from their tuning guide) just use rep movsb
// worse than libc memcpy.

#ifndef HAVE_ASM_MEMCPY
#define asm_memcpy(a,b,c) memcpy((a), (b), (c))
#endif

template<class T>
const ExtentType::byte *uncompactCopy(const vector<ExtentType::nullCompactInfo> &nci, 
                                      ExtentType::byte *to, const ExtentType::byte *from, 
                                      const ExtentType::byte *from_end) {
    (void)from_end; // force "use" even in non-debug mode

    // Split the loop so that we only do the alignment portion of the
    // code at most once.
    typedef vector<ExtentType::nullCompactInfo>::const_iterator nciiT;
    // copy the n byte things
    nciiT i = nci.begin(); 
    nciiT end = nci.end();
    for(; i != end; ++i) {
	DEBUG_INVARIANT(i->size == sizeof(T), "internal");
	if (!compactIsNull(to, *i)) {
	    size_t tmp = reinterpret_cast<size_t>(from);
	    tmp += sizeof(T) - 1;
	    tmp = tmp & ~(sizeof(T)-1);
	    from = reinterpret_cast<const ExtentType::byte *>(tmp);

	    goto copySome;
	}
    }
    return from;

 copySome:
    
    DEBUG_INVARIANT(from + sizeof(T) <= from_end, "internal");
    *reinterpret_cast<T *>(to + i->offset) =
	*reinterpret_cast<const T *>(from);
    from += sizeof(T);
    ++i;

    for(; i != end; ++i) {
	DEBUG_INVARIANT(i->size == sizeof(T), "internal");
	if (compactIsNull(to, *i)) {
	    continue;
	}
	DEBUG_INVARIANT(from + sizeof(T) <= from_end, "internal");
	*reinterpret_cast<T *>(to + i->offset) =
	    *reinterpret_cast<const T *>(from);
	from += sizeof(T);
    }
    return from;
}

void Extent::uncompactNulls(Extent::ByteArray &fixed_coded, int32_t &size) {
    INVARIANT(type.getPackNullCompact() == ExtentType::CompactNonBool, "bad");
    Extent::ByteArray into;
    INVARIANT(static_cast<size_t>(size) <= fixed_coded.size(), "internal"); 
    into.resize(fixed_coded.size(), true); // need to zero fill padding
    // TODO: benchmark doing the partial zero fills during the copy
    // loop; may or may not be faster especially given we have to
    // zero fill null values.

    // Dense packing the booleans is only really a win if we manage to
    // save a character; it's also going to cost a lot of cpu time, so
    // we choose to not do it.  I guess if we found something that had
    // lots of nullable booleans it could become worth it.

    const byte *from = fixed_coded.begin();
    const byte *from_end = fixed_coded.begin() + size;
    byte *to = into.begin();
    size = into.size(); 
    INVARIANT(type.rep.bool_bytes > 0, "?");
    // If we want to not potentially seg fault on bad input, then we
    // need to turn the from debug invariants into invariants and
    // check that we aren't running off the end early.  The invariant
    // at the end will catch it overall, so we still can't go "wrong"
    while(from < from_end && to < into.end()) {
	if (debug_compact) {
	    cout << format("uncompact from@%d/%d to@%d row %d/%d\n")
		% (from - fixed_coded.begin()) % (from_end - fixed_coded.begin())
		% (to - into.begin())
		% ((to - into.begin())/type.rep.fixed_record_size)
		% (size / type.rep.fixed_record_size);
	}
	DEBUG_INVARIANT(to + type.rep.fixed_record_size <= into.end(), 
			"internal");
	DEBUG_INVARIANT(from + type.rep.bool_bytes <= from_end, "internal");
	    
	asm_memcpy(to, from, type.rep.bool_bytes);
	from += type.rep.bool_bytes;

	typedef vector<ExtentType::nullCompactInfo>::const_iterator nciiT;
	// copy the bytes...
	from = uncompactCopy<byte>(type.rep.nonbool_compact_info_size1, 
				   to, from, from_end);

	from = uncompactCopy<uint32_t>(type.rep.nonbool_compact_info_size4, 
				       to, from, from_end);

	from = uncompactCopy<uint64_t>(type.rep.nonbool_compact_info_size8, 
				       to, from, from_end);

	to += type.rep.fixed_record_size;
    }
    INVARIANT(from == from_end && to == into.end(), "internal");
    fixed_coded.swap(into);
    if (debug_compact) {
	cout << format("recovered %s\n")
	    % hexstring(string((char *)fixed_coded.begin(), fixed_coded.size()));
    }
}

static const uint32_t max_packed_size = 512*1024*1024;

static const unsigned variable_sizes_batch_size = 1024;

// Note: Can't split this into pack fixed and pack variable because 
// packing the variable data can involve updating the fixed dat

uint32_t Extent::packData(Extent::ByteArray &into, uint32_t compression_modes, 
			  uint32_t compression_level, uint32_t *header_packed, 
			  uint32_t *fixed_packed, uint32_t *variable_packed) {
    // Don't need to zero the coded arrays as we will be filling them
    // all in.
    Extent::ByteArray fixed_coded;
    fixed_coded.resize(fixeddata.size(), false);
    Extent::ByteArray variable_coded;
    variable_coded.resize(variabledata.size(), false);
    SINVARIANT(variabledata.size() >= 4);

    HashTable<variableDuplicateEliminate, 
	variableDuplicateEliminate_Hash, 
	variableDuplicateEliminate_Equal> vardupelim;

    memcpy(fixed_coded.begin(), fixeddata.begin(), fixeddata.size());
    vector<bool> warnings;
    warnings.resize(type.rep.field_info.size(),false);
    // copy so that packing is thread safe
    vector<ExtentType::pack_self_relativeT> psr_copy 
	= type.rep.pack_self_relative;
    for(unsigned int j=0; j<type.rep.pack_self_relative.size(); ++j) {
	INVARIANT(psr_copy[j].field_num < type.rep.field_info.size(), "whoa");
	psr_copy[j].double_prev_v = 0; // shouldn't be necessary
	psr_copy[j].int32_prev_v = 0;
	psr_copy[j].int64_prev_v = 0;
    }
    uint32_t nrecords = 0;
    byte *variable_data_pos = variable_coded.begin();
    *(int32 *)variable_data_pos = 0;
    variable_data_pos += 4;
    bool null_compact = type.getPackNullCompact() != ExtentType::CompactNo;
    for(Extent::ByteArray::iterator fixed_record = fixed_coded.begin();
	fixed_record != fixed_coded.end(); 
	fixed_record += type.rep.fixed_record_size) {
	INVARIANT(fixed_record < fixed_coded.end(),"internal error");
	++nrecords;
	
	if (null_compact) {
	    // Might want to always do this -- except it seems unlikely people
	    // would commonly fill in a value and then null it.
	    //
	    // Need to zero fill these as when we do null compaction,
	    // we will stuff zeros in to all null fields, and if
	    // someone did relative packing we need it to unpack
	    // properly.

	    typedef vector<ExtentType::nullCompactInfo>::const_iterator nciiT;
	    for(nciiT j = type.rep.nonbool_compact_info_size1.begin(); 
		j != type.rep.nonbool_compact_info_size1.end(); ++j) {
		if (!compactIsNull(fixed_record, *j)) 
		    continue;
		ExtentType::byte *raw = static_cast<unsigned char *>(fixed_record + j->offset);
		*raw = 0;
	    }

	    for(nciiT j = type.rep.nonbool_compact_info_size4.begin(); 
		j != type.rep.nonbool_compact_info_size4.end(); ++j) {
		if (!compactIsNull(fixed_record, *j)) 
		    continue;
		ExtentType::byte *raw = static_cast<unsigned char *>(fixed_record + j->offset);
		*reinterpret_cast<int32_t *>(raw) = 0;
	    }

	    for(nciiT j = type.rep.nonbool_compact_info_size8.begin(); 
		j != type.rep.nonbool_compact_info_size8.end(); ++j) {
		if (!compactIsNull(fixed_record, *j)) 
		    continue;
		ExtentType::byte *raw = static_cast<unsigned char *>(fixed_record + j->offset);
		*reinterpret_cast<int64_t *>(raw) = 0;
	    }
	}

	// pack variable sized fields ...
	for(unsigned int j=0; j < type.rep.variable32_field_columns.size(); ++j) {
	    int field = type.rep.variable32_field_columns[j];
	    int offset = type.rep.field_info[field].offset;
	    int varoffset = Variable32Field::getVarOffset(&(*fixed_record), offset);
	    Variable32Field::selfcheck(variabledata, varoffset);
	    int32 size = Variable32Field::size(variabledata, varoffset);
	    int32 roundup = Variable32Field::roundupSize(size);
	    if (size == 0) {
		SINVARIANT(varoffset == 0);
	    } else {
		int32 packed_varoffset = -1;
		bool unique = type.rep.field_info[field].unique;
		variableDuplicateEliminate v(variabledata.begin() + varoffset);
		variableDuplicateEliminate *vde = unique ? vardupelim.lookup(v) : NULL;
		if (vde != NULL) { // present
		    packed_varoffset = vde->varbits - variable_coded.begin();
		    DEBUG_SINVARIANT(static_cast<size_t>(packed_varoffset) < variable_coded.size());
		} else {
		    DEBUG_SINVARIANT(static_cast<size_t>(variable_data_pos + 4 + roundup 
							 - variable_coded.begin())
				     <= variable_coded.size());
		    memcpy(variable_data_pos, variabledata.begin() + varoffset, 4 + roundup);
		    packed_varoffset = variable_data_pos - variable_coded.begin();
		    if (unique) {
			v.varbits = variable_data_pos;
			vardupelim.add(v);
		    }
		    
		    variable_data_pos += 4 + roundup;
		}		    
		INVARIANT((packed_varoffset + 4) % 8 == 0, boost::format("bad packing offset %d")
			  % packed_varoffset);
		*(int32 *)(fixed_record + offset) = packed_varoffset;
	    } 
	}
	// pack other relative fields ...  do the packing in reverse
	// order so that the base field in each packing is still in
	// unpacked form
	for(int j=type.rep.pack_other_relative.size()-1;j>=0;--j) {
	    const ExtentType::fieldInfo &field(type.rep.field_info[type.rep.pack_other_relative[j].field_num]);
	    if (null_compact && compactIsNull(fixed_record, 
					      *field.null_compact_info)) {
		// Don't overwrite nulls, must remain 0.
		continue;
	    }
	    int base_field = type.rep.pack_other_relative[j].base_field_num;
	    int field_offset = field.offset;
	    DEBUG_INVARIANT(field_offset < type.rep.fixed_record_size, "bad");
	    switch(field.type)
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(fixed_record + field_offset);
		    double base_v = 
			*(double *)(fixed_record + 
				    type.rep.field_info[base_field].offset);
		    *(double *)(fixed_record + field_offset) = v - base_v;
		}
		break;
		case ExtentType::ft_int32: { 
		    int32 v = *(int32 *)(fixed_record + field_offset);
		    int32 base_v = 
			*(int32 *)(fixed_record + 
				   type.rep.field_info[base_field].offset);
		    *(int32 *)(fixed_record + field_offset) = v - base_v;
		}
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(fixed_record + field_offset);
		    int64 base_v = 
			*(int64 *)(fixed_record + 
				   type.rep.field_info[base_field].offset);
		    *(int64 *)(fixed_record + field_offset) = v - base_v;
		}
		break;

		default:
		    FATAL_ERROR("Internal error");
		}
	}
	// pack self relative ...
	for(unsigned int j=0;j<psr_copy.size();++j) {
	    unsigned field_num = psr_copy[j].field_num;
	    const ExtentType::fieldInfo &field(type.rep.field_info[field_num]);
	    if (null_compact && compactIsNull(fixed_record, 
					      *field.null_compact_info)) {
		// Don't overwrite nulls, must remain 0.
		continue;
	    }
	    int offset = field.offset;
	    DEBUG_INVARIANT(offset < type.rep.fixed_record_size, "bad");
	    switch(field.type) 
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(fixed_record + offset);
		    *(double *)(fixed_record + offset) = v - psr_copy[j].double_prev_v;
		    psr_copy[j].double_prev_v = v;
		}
		break;
		case ExtentType::ft_int32: {
		    int32 v = *(int32 *)(fixed_record + offset);
		    *(int32 *)(fixed_record + offset) = v - psr_copy[j].int32_prev_v;
		    psr_copy[j].int32_prev_v = v;
		}
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(fixed_record + offset);
		    *(int64 *)(fixed_record + offset) = v - psr_copy[j].int64_prev_v;
		    psr_copy[j].int64_prev_v = v;
		}
		break;
		default:
		    INVARIANT(field_num < type.rep.field_info.size(), 
			      boost::format("bad field number %d > %d record %d") 
			      % field_num % type.rep.field_info.size() 
			      % nrecords);

		    FATAL_ERROR(boost::format("Internal Error: unrecognized field type %d for field %s (#%d) offset %d in type %s")
				% field.type % field.name
				% field_num % offset % type.rep.name);
		}
	}
	// pack scaled fields ...
	for(unsigned int j=0;j<type.rep.pack_scale.size();++j) {
	    int field = type.rep.pack_scale[j].field_num;
	    INVARIANT(type.rep.field_info[field].type == ExtentType::ft_double,
		      "internal error, scaled only supported for ft_double");
	    int offset = type.rep.field_info[field].offset;
	    double multiplier = type.rep.pack_scale[j].multiplier;
	    double v = *(double *)(fixed_record + offset);
	    double scaled = v * multiplier;
	    double rounded = round(scaled);
	    if (fabs(scaled - rounded) > 0.1 && warnings[field] == false) {
		cerr << boost::format("Warning, while packing field %s of record %d, error was > 10%%:\n  (%.10g / %.10g = %.2f, round() = %.0f)\n")
		    % type.rep.field_info[field].name % nrecords
		    % v % (1.0/multiplier) % scaled % rounded;
		warnings[field] = true;
	    }
	    *(double *)(fixed_record + offset) = rounded;
	}
    }
    // unfortunately have to take the hash after we do all of the
    // sundry conversions, as the conversions are not perfectly
    // reversable, especially the scaling conversion which is
    // deliberately not precisely reversable
    SINVARIANT(fixed_coded.size() == type.rep.fixed_record_size * nrecords);
    uint32_t bjhash = lintel::bobJenkinsHash(1972, fixed_coded.begin(),
					     type.rep.fixed_record_size * nrecords);

    if (type.getPackNullCompact() != ExtentType::CompactNo) {
	// do this after we do the fixed hash, so the checksum will
	// verify this is reversable.

	compactNulls(fixed_coded);
    }

    SINVARIANT(static_cast<size_t>(variable_data_pos - variable_coded.begin()) 
	       <= variable_coded.size())
    variable_coded.resize(variable_data_pos - variable_coded.begin());

    bjhash = lintel::bobJenkinsHash(bjhash, variable_coded.begin(), variable_coded.size());
    vector<int32> variable_sizes;
    variable_sizes.reserve(variable_sizes_batch_size);
    byte *endvarpos = variable_coded.begin() + variable_coded.size();
    for(byte *curvarpos = variable_coded.begin(4);curvarpos != endvarpos;) {
	int32 size = *(int32 *)curvarpos;
	variable_sizes.push_back(size);
	if (variable_sizes.size() == variable_sizes_batch_size) {
	    bjhash = lintel::bobJenkinsHash(bjhash, &(variable_sizes[0]),
					    4*variable_sizes_batch_size);
	    variable_sizes.resize(0);
	}
	curvarpos += 4 + Variable32Field::roundupSize(size);
	SINVARIANT(curvarpos <= endvarpos);
    }
    bjhash = lintel::bobJenkinsHash(bjhash, &(variable_sizes[0]), 4*variable_sizes.size());
    variable_sizes.resize(0);

    byte compressed_fixed_mode;
    Extent::ByteArray *compressed_fixed 
	= compressBytes(fixed_coded.begin(),fixed_coded.size(),
			compression_modes, compression_level,
			&compressed_fixed_mode);
    byte compressed_variable_mode;
    Extent::ByteArray *compressed_variable;
    // +4, -4 avoids packing the 0 bytes at the beginning of the
    // variable coded stuff since that is fixed
    compressed_variable 
	= compressBytes(variable_coded.begin() + 4,
			variable_coded.size() - 4,
			compression_modes, compression_level,
			&compressed_variable_mode);

    int headersize = 6*4+4*1+type.getName().size();
    headersize += (4 - headersize % 4) % 4;
    INVARIANT(compressed_fixed->size() < max_packed_size 
              && compressed_variable->size() < max_packed_size,
              boost::format("very large packed sizes (>%d bytes) indicates misuse: sizes=%d/%d")
              % max_packed_size % compressed_fixed->size() % compressed_variable->size());
    int extentsize = headersize;
    extentsize += compressed_fixed->size();
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_variable->size();
    extentsize += (4 - extentsize % 4) % 4;
    into.resize(extentsize, false);

    byte *l = into.begin();
    *(int32 *)l = compressed_fixed->size(); l += 4;
    *(int32 *)l = compressed_variable->size(); l += 4;
    *(int32 *)l = nrecords; l += 4;
    *(int32 *)l = variable_coded.size(); l += 4;
    *(int32 *)l = 0; l += 4; // compressed adler32 digest
    *(int32 *)l = bjhash; l += 4;
    *l = compressed_fixed_mode; l += 1;
    *l = compressed_variable_mode; l += 1;
    *l = (byte)type.getName().size(); l += 1;
    *l = 0; l += 1;
    memcpy(l, type.getName().data(), type.getName().size()); l += type.getName().size();
    // TODO: verify that aligning speeds up the copy, I'm 90% sure
    // that's why it was done here since we will always copy out the
    // two parts when we read the extent back in.
    int align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    memcpy(l,compressed_fixed->begin(),compressed_fixed->size()); l += compressed_fixed->size();
    align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    memcpy(l,compressed_variable->begin(),compressed_variable->size()); l += compressed_variable->size();
    align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    SINVARIANT(l - into.begin() == extentsize);

    // adler32 everything but the compressed digest
    uLong adler32sum = adler32(0L, Z_NULL, 0);
    adler32sum = adler32(adler32sum, into.begin(), 4*4);
    adler32sum = adler32(adler32sum, into.begin() + 5*4, into.size()-5*4);
    *(int32 *)(into.begin() + 4*4) = adler32sum;
    if (false) cout << boost::format("final coded size %d bytes\n") % into.size();
    if (header_packed != NULL) *header_packed = headersize;
    if (fixed_packed != NULL) *fixed_packed = fixed_coded.size();
    if (variable_packed != NULL) *variable_packed = variable_coded.size();
    delete compressed_fixed;
    delete compressed_variable;
    return bjhash ^ static_cast<uint32_t>(adler32sum);
}

bool Extent::packBZ2(byte *input, int32 inputsize,
                     Extent::ByteArray &into, int compression_level) {
#if DATASERIES_ENABLE_BZIP2    
    if (into.size() == 0) {
	into.resize(inputsize, false);
    }

    unsigned int outsize = into.size();
    int ret = BZ2_bzBuffToBuffCompress((char *)into.begin(),&outsize,
				       (char *)input,inputsize,
				       compression_level,0,0);
    if (ret == BZ_OK) {
	INVARIANT(outsize <= into.size(), "internal error, outsize is bad");
	into.resize(outsize);
	return true;
    }
    INVARIANT(ret == BZ_OUTBUFF_FULL,
	      boost::format("Whoa, got unexpected libbz2 error %d") % ret);
#endif
    return false;
}

bool Extent::packZLib(byte *input, int32 inputsize,
                      Extent::ByteArray &into, int compression_level) {
    if (into.size() == 0) {
	into.resize(inputsize, false);
    }
    uLongf outsize = into.size();
    int ret = compress2((Bytef *)into.begin(),&outsize,
			(const Bytef *)input,inputsize,
			compression_level);
    if (ret == Z_OK) {
	INVARIANT(outsize <= into.size(), "internal error, outsize is bad");
	into.resize(outsize);
	return true;
    }
    INVARIANT(ret == Z_BUF_ERROR,
	      boost::format("Whoa, got unexpected zlib error %d") % ret);
    return false;
}

bool Extent::packLZO(byte *input, int32 inputsize,
                     Extent::ByteArray &into, int compression_level) {
#if DATASERIES_ENABLE_LZO
    into.resize(inputsize + inputsize/64 + 16 + 3, false);
    lzo_uint out_len = 0;

    lzo_byte *work_memory = new lzo_byte[LZO1X_999_MEM_COMPRESS];
    int ret = lzo1x_999_compress_level((lzo_byte *)input, inputsize,
				       (lzo_byte *)into.begin(), &out_len,
				       work_memory, NULL, 0, 0, compression_level);
    INVARIANT(ret == LZO_E_OK,
	      boost::format("internal error: lzo compression failed (%d)")
	      % ret);
    INVARIANT(out_len < into.size(),
	      boost::format("internal error: lzo compression too large %d >= %d\n")
	      % out_len % into.size());
    
    // Might consider calling the optimize function, but the usage is a 
    // little confusing; it appears that it would need another output
    // buffer
    delete [] work_memory;
	
    into.resize(out_len);
    if ((int32)out_len >= inputsize)
	return false;

    return true;
#else
    return false;
#endif
}

bool Extent::packLZF(byte *input, int32 inputsize,
                     Extent::ByteArray &into, int compression_level) {
    if (into.size() == 0) {
	into.resize(inputsize, false);
    }

    unsigned int ret = lzf_compress(input,inputsize,
				    (void *)into.begin(),into.size());
    if (ret == 0) {
	return false;
    }

    into.resize(ret);
    return true;
}

// TODO: test that this works, but I believe that if we do a resize on
// the extent that is about to be used when we pass it in to the sub
// pack functions then the compression algorithms will stop early if
// they can't compress into the smaller amount of space.

Extent::ByteArray *Extent::compressBytes(byte *input, int32 input_size,
                                         int compression_modes,
                                         int compression_level, byte *mode) {
    Extent::ByteArray *best_packed = NULL;
    *mode = 0;
    if (input_size == 0) {
	return new Extent::ByteArray;
    }
    // lzo coding doesn't understand how to limit the amount of memory
    // used, so try that one first
    if ((compression_modes & compress_lzo)) {
	Extent::ByteArray *lzo_pack = new Extent::ByteArray;
	if (packLZO(input,input_size,*lzo_pack,compression_level)) {
	    if (false) cout << boost::format("lzo packing goes to %d bytes\n") % lzo_pack->size();
	    if (best_packed == NULL || lzo_pack->size() < best_packed->size()) {
		best_packed = lzo_pack;
		*mode = compress_mode_lzo;
	    } 
	}
	if (best_packed != lzo_pack) { // we weren't best
	    delete lzo_pack;
	}
    }
    // lzf compresses really fast, try that one second
    if ((compression_modes & compress_lzf)) {
	Extent::ByteArray *lzf_pack = new Extent::ByteArray;
	if (packLZF(input,input_size,*lzf_pack,compression_level)) {
	    if (false) cout << boost::format("lzf packing goes to %d bytes\n") % lzf_pack->size();
	    if (best_packed == NULL || lzf_pack->size() < best_packed->size()) {
		best_packed = lzf_pack;
		*mode = compress_mode_lzf;
	    } 
	}
	if (best_packed != lzf_pack) { // we weren't best
	    delete lzf_pack;
	}
    }
    
    // bz2 tends to pack the best if used, so try this one next
    if ((compression_modes & compress_bz2)) {
	Extent::ByteArray *bz2_pack = new Extent::ByteArray;
	bz2_pack->resize(best_packed == NULL ? input_size : best_packed->size(), false);
	if (packBZ2(input,input_size,*bz2_pack,compression_level)) {
	    if (false) cout << boost::format("bz2 packing goes to %d bytes\n") % bz2_pack->size();
	    if (best_packed == NULL || bz2_pack->size() < best_packed->size()) {
		delete best_packed;
		best_packed = bz2_pack;
		*mode = compress_mode_bz2;
	    }
	}
	if (best_packed != bz2_pack) { // we weren't best
	    delete bz2_pack;
	}
    }
    // try zlib last...
    if ((compression_modes & compress_zlib)) {
	Extent::ByteArray *zlib_pack = new Extent::ByteArray;
	zlib_pack->resize(best_packed == NULL ? input_size: best_packed->size(), false);
	if (packZLib(input,input_size,*zlib_pack,compression_level)) {
	    if (false) cout << boost::format("zlib packing goes to %d bytes\n") % zlib_pack->size();
	    if (best_packed == NULL || zlib_pack->size() < best_packed->size()) {
		delete best_packed;
		best_packed = zlib_pack;
		*mode = compress_mode_zlib;
	    }
	}
	if (best_packed != zlib_pack) { // we weren't best
	    delete zlib_pack;
	}
    }
    if (best_packed == NULL) {
	// must be no coding, or all compression algorithms worked badly
	best_packed = new Extent::ByteArray;
	best_packed->resize(input_size, false);
	memcpy(best_packed->begin(),input,input_size);
    }
    return best_packed;
}

int32_t Extent::uncompressBytes(byte *into, byte *from, byte compression_mode, int32 intosize,
                                int32 fromsize) {
    int32 outsize = -1;
    if (compression_mode == compress_mode_none) {
	outsize = fromsize;
        SINVARIANT(intosize >= fromsize);
	memcpy(into, from, fromsize);
#if DATASERIES_ENABLE_LZO
    } else if (compression_mode == compress_mode_lzo) {
	lzo_uint orig_len = intosize;
	int ret = lzo1x_decompress_safe((const lzo_byte *)from,
					fromsize,
					(lzo_byte *)into,
					&orig_len, NULL);
	INVARIANT(ret == LZO_E_OK,
		  format("Error decompressing extent (%d,%d =? %d)!")
		  % ret % orig_len % intosize);
	outsize = orig_len;
#endif
    } else if (compression_mode == compress_mode_zlib) {
	uLongf destlen = intosize;
	int ret = uncompress((Bytef *)into, &destlen,
			     (const Bytef *)from, fromsize);
	INVARIANT(ret == Z_OK, "Error decompressing extent!");
	outsize = destlen;
#if DATASERIES_ENABLE_BZIP2
    } else if (compression_mode == compress_mode_bz2) {
	unsigned int destlen = intosize;
	int ret = BZ2_bzBuffToBuffDecompress((char *)into,
					     &destlen,
					     (char *)from,
					     fromsize,
					     0,0);
	INVARIANT(ret == BZ_OK, "Error decompressing extent!");
	outsize = destlen;
#endif
    } else if (compression_mode == compress_mode_lzf) {
	unsigned int destlen = lzf_decompress((const void *)from, fromsize,
					       (void *)into, intosize);
	outsize = destlen;
    } else {
	string mode_name("unknown");
	if (compression_mode == compress_mode_lzo) {
	    mode_name = "lzo";
	} else if (compression_mode == compress_mode_zlib) {
	    mode_name = "zlib";
	} else if (compression_mode == compress_mode_bz2) {
	    mode_name = "bz2";
	} else if (compression_mode == compress_mode_lzf) {
	    mode_name = "lzf";
	} 
	FATAL_ERROR(format("Unknown/disabled compression method %s (#%d)")
		    % mode_name % static_cast<int>(compression_mode));
    }
    INVARIANT(outsize >= 0 && outsize <= intosize, 
	      format("bad uncompressbytes %d/%d") % intosize % fromsize);
    return outsize;
}

#define TIME_UNPACKING(x)

const string Extent::getPackedExtentType(const Extent::ByteArray &from) {
    INVARIANT(from.size() > (6*4+2), "Invalid extent data, too small.");

    byte type_name_len = from[6*4+2];

    unsigned header_len = 6*4+4+type_name_len;
    header_len += (4 - (header_len % 4))%4;
    INVARIANT(from.size() >= header_len, "Invalid extent data, too small");

    string type_name((char *)from.begin() + (6*4+4), (int)type_name_len);
    return type_name;
}

void Extent::unpackData(Extent::ByteArray &from, bool fix_endianness) {
    if (!did_checks_init) {
	setReadChecksFromEnv();
    }
    INVARIANT(type.getName() == getPackedExtentType(from), 
	      "Internal: type mismatch") ;

    TIME_UNPACKING(Clock::Tdbl time_start = Clock::tod());
    INVARIANT(from.size() > (6*4+2), "Invalid extent data, too small.");

    uLong adler32sum = adler32(0L, Z_NULL, 0);
    if (preuncompress_check) {
	adler32sum = adler32(adler32sum, from.begin(), 4*4);
	adler32sum = adler32(adler32sum, from.begin() + 5*4, from.size()-5*4);
    }
    if (fix_endianness) {
	for(int i=0;i<6*4;i+=4) {
	    Extent::flip4bytes(from.begin() + i);
	}
    }
    if (preuncompress_check) {
	INVARIANT(*(int32 *)(from.begin() + 4*4) == (int32)adler32sum,
		  boost::format("Invalid extent data, adler32 digest"
				" mismatch on compressed data %x != %x")
		  % *(int32 *)(from.begin() + 4*4) % (int32)adler32sum);
    }
    TIME_UNPACKING(Clock::Tdbl time_upc = Clock::tod());
    int32 compressed_fixed_size = *(int32 *)from.begin();
    int32 compressed_variable_size = *(int32 *)(from.begin() + 4);
    int32 nrecords = *(int32 *)(from.begin() + 8);
    int32 variable_size = *(int32 *)(from.begin() + 12);
    byte compressed_fixed_mode = from[6*4];
    byte compressed_variable_mode = from[6*4+1];
    byte type_name_len = from[6*4+2];
    
    uint32_t header_len = 6*4+4+type_name_len;
    header_len += (4 - (header_len % 4))%4;
    INVARIANT(from.size() >= header_len, "Invalid extent data, too small");

    byte *compressed_fixed_begin = from.begin() + header_len;
    int32 rounded_fixed = compressed_fixed_size;
    rounded_fixed += (4- (rounded_fixed %4))%4;
    byte *compressed_variable_begin = compressed_fixed_begin + rounded_fixed;
    int32 rounded_variable = compressed_variable_size;
    rounded_variable += (4-(rounded_variable%4))%4;

    INVARIANT(header_len + rounded_fixed + rounded_variable == from.size(),
	      "Invalid extent data");

    fixeddata.resize(nrecords * type.rep.fixed_record_size, false);
    int32 fixed_uncompressed_size
	= uncompressBytes(fixeddata.begin(),compressed_fixed_begin,
			  compressed_fixed_mode,
			  nrecords * type.rep.fixed_record_size,
			  compressed_fixed_size);
    if (type.getPackNullCompact() != ExtentType::CompactNo) {
	uncompactNulls(fixeddata, fixed_uncompressed_size);
    }
    INVARIANT(fixed_uncompressed_size == nrecords * type.rep.fixed_record_size, "internal");
    
    variabledata.resize(variable_size, false);
    INVARIANT(variable_size >= 4, "error unpacking, invalid variable size");
    *(int32 *)variabledata.begin() = 0;
    int32 variable_uncompressed_size
	= uncompressBytes(variabledata.begin()+4, compressed_variable_begin,
			  compressed_variable_mode,
			  variable_size-4, compressed_variable_size);
    INVARIANT(variable_uncompressed_size == variable_size - 4, "internal");
    uint32_t bjhash = 0;
    if (postuncompress_check) {
	bjhash = lintel::bobJenkinsHash(1972, fixeddata.begin(), fixeddata.size());
	bjhash = lintel::bobJenkinsHash(bjhash, variabledata.begin(), variabledata.size());
    }
    vector<int32> variable_sizes;
    variable_sizes.reserve(variable_sizes_batch_size);
    byte *endvarpos = variabledata.begin() + variabledata.size();
    for(byte *curvarpos = &variabledata[4];curvarpos != endvarpos;) {
	int32 size = *(int32 *)curvarpos;
	if (postuncompress_check) {
	    variable_sizes.push_back(size);

	    if (variable_sizes.size() == variable_sizes_batch_size) {
		bjhash = lintel::bobJenkinsHash(bjhash, &(variable_sizes[0]),
						4*variable_sizes_batch_size);
		variable_sizes.resize(0);
	    }
	}
	if (fix_endianness) {
	    size = Extent::flip4bytes(size);
	    *(int32 *)curvarpos = size;
	}
	curvarpos += 4 + Variable32Field::roundupSize(size);
	INVARIANT(curvarpos <= endvarpos,"internal error on variable data");
    }
    if (postuncompress_check) {
	bjhash = lintel::bobJenkinsHash(bjhash,&(variable_sizes[0]),4*variable_sizes.size());
    }
    variable_sizes.resize(0);
    INVARIANT(postuncompress_check == false 
	      || *(int32 *)(from.begin() + 5*4) == (int32)bjhash,
	      "final partially unpacked hash check failed");
    
    vector<ExtentType::pack_self_relativeT> psr_copy 
	= type.rep.pack_self_relative;
    for(unsigned int j=0;j<type.rep.pack_self_relative.size();++j) {
	SINVARIANT(psr_copy[j].field_num < type.rep.field_info.size());
	
	SINVARIANT(psr_copy[j].double_prev_v == 0 &&
		   psr_copy[j].int32_prev_v == 0 &&
		   psr_copy[j].int64_prev_v == 0);
    }
    TIME_UNPACKING(Clock::Tdbl time_postuc = Clock::tod());
    int record_count = 0;
    // 2004-09-26: each one of these is worth a small speedup, 0.5-1%
    // or so I'd guess, while not inherently worth it, the fix was
    // made when profiling accidentally with the debugging library, 
    // and there is no point in removing the fix.
    const size_t type_variable32_field_columns_size 
	= type.rep.variable32_field_columns.size();
    const size_t type_pack_scale_size = type.rep.pack_scale.size();
    const size_t type_pack_self_relative_size = psr_copy.size();
    const size_t type_pack_other_relative_size 
	= type.rep.pack_other_relative.size();
    const bool null_compact = type.getPackNullCompact() != ExtentType::CompactNo;
    for(ExtentSeries::iterator pos(this); pos.morerecords(); ++pos) {
	++record_count;
	if (fix_endianness) {
	    for(unsigned int j=0; j<type.rep.field_info.size(); j++) {
		switch(type.rep.field_info[j].type)
		    {
		    case ExtentType::ft_bool: 
		    case ExtentType::ft_byte:
			break;
		    case ExtentType::ft_int32:
		    case ExtentType::ft_variable32:
			Extent::flip4bytes(pos.record_start() 
					   + type.rep.field_info[j].offset);
			break;
		    case ExtentType::ft_int64:
		    case ExtentType::ft_double:
			Extent::flip8bytes(pos.record_start() 
					   + type.rep.field_info[j].offset);
			break;
		    default:
			FATAL_ERROR(boost::format("unknown field type %d for fix_endianness") % type.rep.field_info[j].type);
			break;
		    }
	    }
	}
	// check variable sized fields ...
	if (unpack_variable32_check) {
	    for(unsigned int j=0;j<type_variable32_field_columns_size;j++) {
		int field = type.rep.variable32_field_columns[j];
		int32 offset = type.rep.field_info[field].offset;
		int32 varoffset 
		    = Variable32Field::getVarOffset(pos.record_start(), 
						    offset);
		// now check with the standard verification routine
		Variable32Field::selfcheck(variabledata,varoffset);
	    }
	}     
	// Unpacking is done in the reverse order as packing.

	// unpack scaled fields ...
	for(unsigned int j=0;j<type_pack_scale_size;++j) {
	    int field = type.rep.pack_scale[j].field_num;
	    INVARIANT(type.rep.field_info[field].type == ExtentType::ft_double,
		      "internal error, scaled only supported for ft_double");
	    int offset = type.rep.field_info[field].offset;
	    double scale = type.rep.pack_scale[j].scale;
	    double v = *(double *)(pos.record_start() + offset);
	    *(double *)(pos.record_start() + offset) = v * scale;
	}

	// unpack self-relative fields ...
	for(unsigned int j=0;j<type_pack_self_relative_size;++j) {
	    unsigned field_num = psr_copy[j].field_num;
	    const ExtentType::fieldInfo &field(type.rep.field_info[field_num]);
	    if (null_compact && compactIsNull(pos.record_start(),
					      *field.null_compact_info)) {
		// Don't overwrite nulls, must remain 0 to unpack properly.
		continue;
	    }
	    int offset = field.offset;
	    switch(field.type) 
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(pos.record_start() + offset) + psr_copy[j].double_prev_v;
		    double multiplier = psr_copy[j].multiplier;
		    double scale = psr_copy[j].scale;
		    v = round(v * multiplier) * scale;
		    *(double *)(pos.record_start() + offset) = v;
		    psr_copy[j].double_prev_v = v;
		}
		break;
		case ExtentType::ft_int32: {
		    int32 v = *(int32 *)(pos.record_start() + offset) + psr_copy[j].int32_prev_v;
		    *(int32 *)(pos.record_start() + offset) = v;
		    psr_copy[j].int32_prev_v = v;
		}		    
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(pos.record_start() + offset) + psr_copy[j].int64_prev_v;
		    *(int64 *)(pos.record_start() + offset) = v;
		    psr_copy[j].int64_prev_v = v;
		}		    
		break;
		default:
		    INVARIANT(field_num < type.rep.field_info.size(), 
			      boost::format("bad field number %d > %d") 
			      % field_num % type.rep.field_info.size());

		    FATAL_ERROR(boost::format("Internal Error: unrecognized field type %d for field %s (#%d) offset %d in type %s")
				% field.type % field.name
				% field_num % offset % type.rep.name);
		}
	}
	// unpack other-relative fields ...
	for(unsigned int j=0;j<type_pack_other_relative_size;++j) {
	    // 2004-09-26 I cannot believe this is worth a 1% speedup (pulling out v).
	    const ExtentType::pack_other_relativeT &v 
		= type.rep.pack_other_relative[j];
	    int field_num = v.field_num;
	    const ExtentType::fieldInfo &field(type.rep.field_info[field_num]);
	    if (null_compact && compactIsNull(pos.record_start(), 
					      *field.null_compact_info)) {
		// Don't overwrite nulls, must remain 0 to unpack properly.
		continue;
	    }
	    int base_field = v.base_field_num;
	    int field_offset = field.offset;
	    byte *base_ptr = pos.record_start()
		+ type.rep.field_info[base_field].offset;
	    switch(field.type)
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(pos.record_start() + field_offset);
		    double base_v = *reinterpret_cast<double *>(base_ptr);
		    *(double *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		case ExtentType::ft_int32: {
		    int32 v = *(int32 *)(pos.record_start() + field_offset);
		    int32 base_v = *reinterpret_cast<int32 *>(base_ptr);
		    *(int32 *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(pos.record_start() + field_offset);
		    int64 base_v = *reinterpret_cast<int64 *>(base_ptr);
		    *(int64 *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		default:
		    FATAL_ERROR("Internal error");
		}
	}
    }	
    INVARIANT(record_count == nrecords,"internal error");
    TIME_UNPACKING(Clock::Tdbl time_done = Clock::tod();
    printf("%d records, unpackcheck %.6g; uncompress %.6g; unpack %.6g\n",
	   nrecords,
	   time_upc - time_start,
	   time_postuc - time_upc,
	   time_done - time_postuc));
}

uint32_t
Extent::unpackedSize(Extent::ByteArray &from, bool fix_endianness, const ExtentType &type) {
    SINVARIANT(from.size() > 16);
    uint32_t nrecords = *reinterpret_cast<uint32_t *>(from.begin() + 8);
    uint32_t variable_size = *reinterpret_cast<uint32_t *>(from.begin() + 12);
    if (fix_endianness) {
	nrecords = flip4bytes(nrecords);
	variable_size = flip4bytes(variable_size);
    }
    return nrecords * type.fixedrecordsize() + variable_size;
}

bool Extent::checkedPread(int fd, off64_t offset, byte *into, int amount, bool eof_ok) {
    ssize_t ret = pread64(fd,into,amount,offset);
    INVARIANT(ret != -1, boost::format("error reading %d bytes: %s") 
	      % amount % strerror(errno));
    if (ret == 0 && eof_ok) {
	return false;
    }
    INVARIANT(ret == amount, boost::format("partial read %d of %d bytes: %s\n")
	      % ret % amount % strerror(errno));
    return true;
}

bool Extent::preadExtent(int fd, off64_t &offset, Extent::ByteArray &into, bool need_bitflip) {
    int prefix_size = 6*4 + 4*1;
    into.resize(prefix_size, false);
    if (checkedPread(fd,offset,into.begin(),prefix_size, true) == false) {
	into.resize(0);
	return false;
    }
    offset += prefix_size;
    byte *l = into.begin();
    int32_t compressed_fixed = *(int32_t *)l; l += 4;
    int32_t compressed_variable = *(int32_t *)l; l += 4;
    int32 typenamelen = into[6*4+2];
    if (need_bitflip) {
	compressed_fixed = flip4bytes(compressed_fixed);
	compressed_variable = flip4bytes(compressed_variable);
    }
    if (compressed_fixed == -1) {
	DataSeriesSink::verifyTail(into.begin(), need_bitflip,"*unknown*");
	return false;
    }
    INVARIANT(compressed_fixed >= 0 && compressed_variable >= 0
	      && typenamelen >= 0, "Error reading extent");
    INVARIANT(static_cast<uint32_t>(compressed_fixed) < max_packed_size 
              && static_cast<uint32_t>(compressed_variable) < max_packed_size,
              boost::format("Excessively large extent is almost definitely corruption sizes=%d/%d")
              % compressed_fixed % compressed_variable);
    uint64_t extentsize = prefix_size+typenamelen;
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_fixed;
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_variable;
    extentsize += (4 - extentsize % 4) % 4;
    LintelLogDebug("Extent/size", boost::format("%d %d %d %d ~= %d") % prefix_size % typenamelen
                   % compressed_fixed % compressed_variable % extentsize);
    into.resize(extentsize, false);
    checkedPread(fd, offset, into.begin() + prefix_size, 
		 extentsize - prefix_size);
    offset += extentsize - prefix_size;
    return true;
}

void Extent::run_flip4bytes(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = Extent::flip4bytes(buf[i]);
    }
}
