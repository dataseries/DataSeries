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

#if _FILE_OFFSET_BITS == 64 && !defined(_LARGEFILE64_SOURCE)
#define pread64 pread
#endif

#ifndef DATASERIES_ENABLE_BZ2
#define DATASERIES_ENABLE_BZ2 1
#endif

#ifndef DATASERIES_ENABLE_ZLIB
#define DATASERIES_ENABLE_ZLIB 1
#endif

#ifndef DATASERIES_ENABLE_LZO
#define DATASERIES_ENABLE_LZO 1
#endif

#ifndef DATASERIES_ENABLE_LZF
#define DATASERIES_ENABLE_LZF 1
#endif

#if DATASERIES_ENABLE_BZ2
#include <bzlib.h>
#endif
#if DATASERIES_ENABLE_ZLIB
#include <zlib.h>
#endif
#if DATASERIES_ENABLE_LZO
#include <lzo1x.h>
#endif
#if DATASERIES_ENABLE_LZF
extern "C" {
#include <lzf.h>
}
#endif

#include <Lintel/HashTable.H>
#include <Lintel/Clock.H>

#include <DataSeries/Extent.H>
#include <DataSeries/ExtentField.H>
#include <DataSeries/DataSeriesFile.H>

extern "C" {
    char *dataseriesVersion() {
	return (char *)VERSION;
    }
}

bool dataseries_enable_preuncompress_check = true;
bool dataseries_enable_postuncompress_check = true;
bool dataseries_enable_unpack_variable32_check = true;

#if DATASERIES_ENABLE_LZO
static int lzo_init = 0;
#endif

void
Extent::init()
{
#if DATASERIES_ENABLE_LZO
    if (lzo_init == 0) {
	AssertAlways(lzo_init() == LZO_E_OK,
		     ("lzo_init() failed ?!\n"));
	lzo_init = 1;
    }
#endif
    // the default offset for variable sized fields is pointed to
    // offset 0; so we set up the 0 entry in the variable sized data
    // to be an 0 sized variable bit.
    variabledata.resize(4);
    // slightly incestuous interaction between Variable32Field and
    // Extent, but probably ok.
    *(int32 *)variabledata.begin() = 0;
}

Extent::Extent(ExtentTypeLibrary &library, 
	       Extent::ByteArray &packeddata,
	       const bool need_bitflip)
    : type(NULL)
{
    init();
    unpackData(library,packeddata,need_bitflip);
}

Extent::Extent(const ExtentType *_type,
	       Extent::ByteArray &packeddata,
	       const bool need_bitflip)
    : type(_type)
{
    init();
    unpackData(type,packeddata,need_bitflip);
}

Extent::Extent(const ExtentType *_type)
    : type(_type)
{
    init();
}

Extent::Extent(const std::string &xmltype)
    : type(new ExtentType(xmltype))
{
    init();
}

Extent::Extent(ExtentSeries &myseries)
    : type(myseries.type)
{
    init();
    if (myseries.extent() == NULL) {
	myseries.setExtent(*this);
    }
}

void
Extent::createRecords(unsigned int nrecords)
{
    fixeddata.resize(fixeddata.size() + nrecords * type->fixed_record_size);
}    

struct variableDuplicateEliminate {
    ExtentType::byte *varbits;
    variableDuplicateEliminate(ExtentType::byte *a) : varbits(a) {}
};

class variableDuplicateEliminate_Equal {
public:
    typedef ExtentType::int32 int32;
    bool operator()(const variableDuplicateEliminate &a, 
		    const variableDuplicateEliminate &b) {
	int32 size_a = *(int32 *)(a.varbits);
	int32 size_b = *(int32 *)(b.varbits);
	if (size_a != size_b) return false;
	return memcmp(a.varbits,b.varbits,size_a + 4) == 0;
    }
};

class variableDuplicateEliminate_Hash {
public:
    typedef ExtentType::int32 int32;
    unsigned int operator()(const variableDuplicateEliminate &a) {
	int32 size_a = *(int32 *)(a.varbits);
	return BobJenkinsHash(1776, a.varbits, 4+size_a);
    }
};

static const int variable_sizes_batch_size = 1024;

uint32_t
Extent::packData(Extent::ByteArray &into,
		 int compression_modes,
		 int compression_level,
		 int *header_packed, int *fixed_packed, int *variable_packed)
{
    Extent::ByteArray fixed_coded;
    fixed_coded.resize(fixeddata.size());
    Extent::ByteArray variable_coded;
    variable_coded.resize(variabledata.size());
    AssertAlways(variabledata.size() >= 4,("internal error\n"));

    HashTable<variableDuplicateEliminate, 
	variableDuplicateEliminate_Hash, 
	variableDuplicateEliminate_Equal> vardupelim;

    memcpy(fixed_coded.begin(), fixeddata.begin(), fixeddata.size());
    std::vector<bool> warnings;
    warnings.resize(type->field_info.size(),false);
    // copy so that packing is thread safe
    std::vector<ExtentType::pack_self_relativeT> psr_copy = type->pack_self_relative;
    for(unsigned int j=0;j<type->pack_self_relative.size();++j) {
	psr_copy[j].double_prev_v = 0; // shouldn't be necessary
	psr_copy[j].int32_prev_v = 0;
	psr_copy[j].int64_prev_v = 0;
    }
    int32 nrecords = 0;
    byte *variable_data_pos = variable_coded.begin();
    *(int32 *)variable_data_pos = 0;
    variable_data_pos += 4;
    for(Extent::ByteArray::iterator fixed_record = fixed_coded.begin();
	fixed_record != fixed_coded.end(); 
	fixed_record += type->fixed_record_size) {
	AssertAlways(fixed_record < fixed_coded.end(),("internal error\n"));
	++nrecords;
	
	// pack variable sized fields ...
	for(unsigned int j=0;j<type->variable32_field_columns.size();j++) {
	    int field = type->variable32_field_columns[j];
	    int offset = type->field_info[field].offset;
	    int varoffset = Variable32Field::getVarOffset(&(*fixed_record),
							  offset);
	    Variable32Field::selfcheck(variabledata,varoffset);
	    int32 size = Variable32Field::size(variabledata,varoffset);
	    int32 roundup = Variable32Field::roundupSize(size);
	    if (size == 0) {
		AssertAlways(varoffset == 0,("internal error\n"));
	    } else {
		memcpy(variable_data_pos, variabledata.begin() + varoffset,
		       4 + roundup);
		int32 packed_varoffset = variable_data_pos - variable_coded.begin();
		if (type->field_info[field].unique) {
		    variableDuplicateEliminate v(variable_data_pos);
		    variableDuplicateEliminate *dup = vardupelim.lookup(v);
		    if (dup == NULL) {
			// not present; add and use space
			vardupelim.add(v);
			variable_data_pos += 4 + roundup;
		    } else {
			// already present, eliminate duplicate
			packed_varoffset = dup->varbits - variable_coded.begin();
		    }
		} else {
		    variable_data_pos += 4 + roundup;
		}
		AssertAlways((packed_varoffset + 4) % 8 == 0,
			     ("bad packing offset %d\n",packed_varoffset));
		*(int32 *)(fixed_record + offset) = packed_varoffset;
	    } 
	}
	// pack other relative fields ...  do the packing in reverse
	// order so that the base field in each packing is still in
	// unpacked form
	for(int j=type->pack_other_relative.size()-1;j>=0;--j) {
	    int field = type->pack_other_relative[j].field_num;
	    int base_field = type->pack_other_relative[j].base_field_num;
	    int field_offset = type->field_info[field].offset;
	    switch(type->field_info[field].type)
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(fixed_record + field_offset);
		    double base_v = *(double *)(fixed_record + type->field_info[base_field].offset);
		    *(double *)(fixed_record + field_offset) = v - base_v;
		}
		break;
		case ExtentType::ft_int32: { 
		    int32 v = *(int32 *)(fixed_record + field_offset);
		    int32 base_v = *(int32 *)(fixed_record + type->field_info[base_field].offset);
		    *(int32 *)(fixed_record + field_offset) = v - base_v;
		}
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(fixed_record + field_offset);
		    int64 base_v = *(int64 *)(fixed_record + type->field_info[base_field].offset);
		    *(int64 *)(fixed_record + field_offset) = v - base_v;
		}
		break;

		default:
		    AssertFatal(("Internal error\n"));
		}
	}
	// pack self relative ...
	for(unsigned int j=0;j<psr_copy.size();++j) {
	    int field = psr_copy[j].field_num;
	    int offset = type->field_info[field].offset;
	    switch(type->field_info[field].type) 
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
		    AssertFatal(("Internal error\n"));
		}
	}
	// pack scaled fields ...
	for(unsigned int j=0;j<type->pack_scale.size();++j) {
	    int field = type->pack_scale[j].field_num;
	    AssertAlways(type->field_info[field].type == ExtentType::ft_double,
			 ("internal error, scaled only supported for ft_double\n"));
	    int offset = type->field_info[field].offset;
	    double multiplier = type->pack_scale[j].multiplier;
	    double v = *(double *)(fixed_record + offset);
	    double scaled = v * multiplier;
	    double rounded = round(scaled);
	    if (fabs(scaled - rounded) > 0.1 && warnings[field] == false) {
		fprintf(stderr,"Warning, while packing field %s of record %d, error was > 10%%:\n  (%.10g / %.10g = %.2f, round() = %.0f)\n",
			type->field_info[field].name.c_str(),nrecords,
			v, 1.0/multiplier, scaled, rounded);
		warnings[field] = true;
	    }
	    *(double *)(fixed_record + offset) = rounded;
	}
    }
    // unfortunately have to take the hash after we do all of the
    // sundry conversions, as the conversions are not perfectly
    // reversable, especially the scaling conversion which is
    // deliberately not precisely reversable
    AssertAlways((int)fixed_coded.size() == type->fixed_record_size * nrecords,
		 ("internal error\n"));
    uint32_t bjhash = BobJenkinsHash(1972,fixed_coded.begin(),type->fixed_record_size * nrecords);
    AssertAlways(variable_data_pos - variable_coded.begin() <= (int)variable_coded.size(),
		 ("Internal error\n"));
    variable_coded.resize(variable_data_pos - variable_coded.begin());

    bjhash = BobJenkinsHash(bjhash,variable_coded.begin(),variable_coded.size());
    std::vector<int32> variable_sizes;
    variable_sizes.reserve(variable_sizes_batch_size);
    byte *endvarpos = variable_coded.begin() + variable_coded.size();
    for(byte *curvarpos = variable_coded.begin(4);curvarpos != endvarpos;) {
	int32 size = *(int32 *)curvarpos;
	variable_sizes.push_back(size);
	if ((int)variable_sizes.size() == variable_sizes_batch_size) {
	    bjhash = BobJenkinsHash(bjhash,&(variable_sizes[0]),4*variable_sizes_batch_size);
	    variable_sizes.resize(0);
	}
	curvarpos += 4 + Variable32Field::roundupSize(size);
	AssertAlways(curvarpos <= endvarpos,("internal error\n"));
    }
    bjhash = BobJenkinsHash(bjhash,&(variable_sizes[0]),4*variable_sizes.size());
    variable_sizes.resize(0);

    byte compressed_fixed_mode;
    Extent::ByteArray *compressed_fixed = packAny(fixed_coded.begin(),fixed_coded.size(),
					     compression_modes,
					     compression_level,
					     &compressed_fixed_mode);
    byte compressed_variable_mode;
    Extent::ByteArray *compressed_variable;
    // +4, -4 avoids packing the 0 bytes at the beginning of the
    // variable coded stuff since that is fixed
    compressed_variable = packAny(variable_coded.begin() + 4,
				  variable_coded.size() - 4,
				  compression_modes,
				  compression_level,
				  &compressed_variable_mode);

    int headersize = 6*4+4*1+type->name.size();
    headersize += (4 - headersize % 4) % 4;
    int extentsize = headersize;
    extentsize += compressed_fixed->size();
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_variable->size();
    extentsize += (4 - extentsize % 4) % 4;
    into.resize(extentsize);

    byte *l = into.begin();
    *(int32 *)l = compressed_fixed->size(); l += 4;
    *(int32 *)l = compressed_variable->size(); l += 4;
    *(int32 *)l = nrecords; l += 4;
    *(int32 *)l = variable_coded.size(); l += 4;
    memset(l,0,4); l += 4; // compressed adler32 digest
    *(int32 *)l = bjhash; l += 4;
    *l = compressed_fixed_mode; l += 1;
    *l = compressed_variable_mode; l += 1;
    *l = (byte)type->name.size(); l += 1;
    *l = 0; l += 1;
    memcpy(l,type->name.data(),type->name.size()); l += type->name.size();
    int align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    memcpy(l,compressed_fixed->begin(),compressed_fixed->size()); l += compressed_fixed->size();
    align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    memcpy(l,compressed_variable->begin(),compressed_variable->size()); l += compressed_variable->size();
    align = (4 - ((l - into.begin()) % 4)) % 4;
    memset(l,0,align); l += align;
    AssertAlways(l - into.begin() == extentsize,
		 ("Internal Error\n"));

    // adler32 everything but the compressed digest
    uLong adler32sum = adler32(0L, Z_NULL, 0);
    adler32sum = adler32(adler32sum, into.begin(), 4*4);
    adler32sum = adler32(adler32sum, into.begin() + 5*4, into.size()-5*4);
    *(int32 *)(into.begin() + 4*4) = adler32sum;
    if (false) printf("final coded size %d bytes\n",into.size());
    if (header_packed != NULL) *header_packed = headersize;
    if (fixed_packed != NULL) *fixed_packed = fixed_coded.size();
    if (variable_packed != NULL) *variable_packed = variable_coded.size();
    delete compressed_fixed;
    delete compressed_variable;
    return bjhash ^ static_cast<uint32_t>(adler32sum);
}

bool
Extent::packBZ2(byte *input, int32 inputsize,
		Extent::ByteArray &into, int compression_level)
{
#if DATASERIES_ENABLE_BZ2    
    if (into.size() == 0) {
	into.resize(inputsize);
    }
    unsigned int outsize = into.size();
    int ret = BZ2_bzBuffToBuffCompress((char *)into.begin(),&outsize,
				       (char *)input,inputsize,
				       compression_level,0,0);
    if (ret == BZ_OK) {
	AssertAlways(outsize <= into.size(),
		     ("internal error, outsize is bad\n"));
	into.resize(outsize);
	return true;
    }
    AssertAlways(ret == BZ_OUTBUFF_FULL,
		 ("Whoa, got unexpected libbz2 error %d\n",ret));
#endif
    return false;
}

bool
Extent::packZLib(byte *input, int32 inputsize,
		 Extent::ByteArray &into, int compression_level)
{
#if DATASERIES_ENABLE_ZLIB
    if (into.size() == 0) {
	into.resize(inputsize);
    }
    uLongf outsize = into.size();
    int ret = compress2((Bytef *)into.begin(),&outsize,
			(const Bytef *)input,inputsize,
			compression_level);
    if (ret == Z_OK) {
	AssertAlways(outsize <= into.size(),
		     ("internal error, outsize is bad\n"));
	into.resize(outsize);
	return true;
    }
    AssertAlways(ret == Z_BUF_ERROR,
		 ("Whoa, got unexpected zlib error %d\n",ret));
#endif
    return false;
}

bool
Extent::packLZO(byte *input, int32 inputsize,
		Extent::ByteArray &into, int compression_level)
{
#if DATASERIES_ENABLE_LZO
    into.resize(inputsize + inputsize/64 + 16 + 3);
    lzo_uint out_len = 0;

    lzo_byte *work_memory = new lzo_byte[LZO1X_999_MEM_COMPRESS];
    int ret = lzo1x_999_compress_level((lzo_byte *)input, inputsize,
				       (lzo_byte *)into.begin(), &out_len,
				       work_memory, NULL, 0, 0, compression_level);
    AssertAlways(ret == LZO_E_OK,
		 ("internal error: lzo compression failed (%d)\n",ret));
    AssertAlways(out_len < into.size(),
		 ("internal error: lzo compression too large %d >= %d\n",
		  out_len, into.size()));
    
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

bool
Extent::packLZF(byte *input, int32 inputsize,
		Extent::ByteArray &into, int compression_level)
{
#if DATASERIES_ENABLE_LZF
    if (into.size() == 0) {
	into.resize(inputsize);
    }

    unsigned int ret = lzf_compress(input,inputsize,
				    (void *)into.begin(),into.size());
    if (ret == 0) {
	return false;
    }

    into.resize(ret);
    return true;
#endif
    return false;
}

Extent::ByteArray *
Extent::packAny(byte *input, int32 input_size,
		int compression_modes,
		int compression_level, byte *mode)
{
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
	    if (false) printf("lzo packing goes to %d bytes\n",lzo_pack->size());
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
	    if (false) printf("lzf packing goes to %d bytes\n",lzf_pack->size());
	    if (best_packed == NULL || lzf_pack->size() < best_packed->size()) {
		best_packed = lzf_pack;
		*mode = compress_mode_lzf;
	    } 
	}
	if (best_packed != lzf_pack) { // we weren't best
	    delete lzf_pack;
	}
    }
    
    // bz2 tends to pack the best if used, so try this one first
    if ((compression_modes & compress_bz2)) {
	Extent::ByteArray *bz2_pack = new Extent::ByteArray;
	bz2_pack->resize(best_packed == NULL ? input_size : best_packed->size());
	if (packBZ2(input,input_size,*bz2_pack,compression_level)) {
	    if (false) printf("bz2 packing goes to %d bytes\n",bz2_pack->size());
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
	zlib_pack->resize(best_packed == NULL ? input_size: best_packed->size());
	if (packZLib(input,input_size,*zlib_pack,compression_level)) {
	    if (false) printf("zlib packing goes to %d bytes\n",zlib_pack->size());
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
	best_packed->resize(input_size);
	memcpy(best_packed->begin(),input,input_size);
    }
    return best_packed;
}

void 
Extent::unpackAny(byte *into, byte *from, 
		  byte compression_mode, int32 intosize,
		  int32 fromsize)
{
    if (compression_mode == compress_mode_none) {
	AssertAlways(intosize == fromsize,("bad unpack any\n"));
	memcpy(into,from,intosize);
#if DATASERIES_ENABLE_LZO
    } else if (compression_mode == compress_mode_lzo) {
	lzo_uint orig_len = intosize;
	int ret = lzo1x_decompress_safe((lzo_byte *)from,
					fromsize,
					(lzo_byte *)into,
					&orig_len, NULL);
	AssertAlways(ret == LZO_E_OK && (int)orig_len == intosize,
		     ("Error decompressing extent (%d,%d =? %d)!\n",
		      ret,orig_len,intosize));
#endif
#if DATASERIES_ENABLE_ZLIB
    } else if (compression_mode == compress_mode_zlib) {
	uLongf destlen = intosize;
	int ret = uncompress((Bytef *)into,
			     &destlen,(const Bytef *)from,
			     fromsize);
	AssertAlways(ret == Z_OK && (int)destlen == intosize,
		     ("Error decompressing extent!\n"));
#endif
#if DATASERIES_ENABLE_BZ2
    } else if (compression_mode == compress_mode_bz2) {
	unsigned int destlen = intosize;
	int ret = BZ2_bzBuffToBuffDecompress((char *)into,
					     &destlen,
					     (char *)from,
					     fromsize,
					     0,0);
	AssertAlways(ret == BZ_OK && (int)destlen == intosize,
		     ("Error decompressing extent!\n"));
#endif
#if DATASERIES_ENABLE_LZF
    } else if (compression_mode == compress_mode_lzf) {
	unsigned int destlen = lzf_decompress((void *)from, fromsize,
					       (void *)into, intosize);
	AssertAlways((int)destlen == intosize,
		     ("Error decompressing extent!\n"));
#endif
    } else {
	char *mode_name = (char *)"unknown";
	if (compression_mode == compress_mode_lzo) {
	    mode_name = (char *)"lzo";
	} else if (compression_mode == compress_mode_zlib) {
	    mode_name = (char *)"zlib";
	} else if (compression_mode == compress_mode_bz2) {
	    mode_name = (char *)"bz2";
	} else if (compression_mode == compress_mode_lzf) {
	    mode_name = (char *)"lzf";
	} 
	AssertFatal(("Unknown/disabled compression method %s (#%d)\n",
		     mode_name, (int)compression_mode));
    }
}

#define TIME_UNPACKING(x)

const std::string
Extent::getPackedExtentType(Extent::ByteArray &from)
{
    AssertAlways(from.size() > (6*4+2),
		 ("Invalid extent data, too small.\n"));

    byte type_name_len = from[6*4+2];

    unsigned header_len = 6*4+4+type_name_len;
    header_len += (4 - (header_len % 4))%4;
    AssertAlways(from.size() >= header_len,("Invalid extent data, too small"));

    std::string type_name((char *)from.begin() + (6*4+4), (int)type_name_len);
    return type_name;
}

void
Extent::unpackData(ExtentTypeLibrary &library,
		   Extent::ByteArray &from,
		   bool fix_endianness)
{
    return unpackData(library.getTypeByName(getPackedExtentType(from)),
		      from,fix_endianness);
}


void
Extent::unpackData(const ExtentType *_type,
		   Extent::ByteArray &from,
		   bool fix_endianness)
{
    TIME_UNPACKING(Clock::Tdbl time_start = Clock::tod());
    AssertAlways(from.size() > (6*4+2),
		 ("Invalid extent data, too small.\n"));

    uLong adler32sum = adler32(0L, Z_NULL, 0);
    if (dataseries_enable_preuncompress_check) {
	adler32sum = adler32(adler32sum, from.begin(), 4*4);
	adler32sum = adler32(adler32sum, from.begin() + 5*4, from.size()-5*4);
    }
    if (fix_endianness) {
	for(int i=0;i<6*4;i+=4) {
	    Extent::flip4bytes(from.begin() + i);
	}
    }
    if (dataseries_enable_preuncompress_check) {
	AssertAlways(*(int32 *)(from.begin() + 4*4) == (int32)adler32sum,
		     ("Invalid extent data, adler32 digest mismatch on compressed data %x != %x\n",*(int32 *)(from.begin() + 4*4),(int32)adler32sum));
    }
    TIME_UNPACKING(Clock::Tdbl time_upc = Clock::tod());
    int32 compressed_fixed_size = *(int32 *)from.begin();
    int32 compressed_variable_size = *(int32 *)(from.begin() + 4);
    int32 nrecords = *(int32 *)(from.begin() + 8);
    int32 variable_size = *(int32 *)(from.begin() + 12);
    byte compressed_fixed_mode = from[6*4];
    byte compressed_variable_mode = from[6*4+1];
    byte type_name_len = from[6*4+2];
    
    unsigned header_len = 6*4+4+type_name_len;
    header_len += (4 - (header_len % 4))%4;
    AssertAlways(from.size() >= header_len,("Invalid extent data, too small"));

    type = _type;
    byte *compressed_fixed_begin = from.begin() + header_len;
    int32 rounded_fixed = compressed_fixed_size;
    rounded_fixed += (4- (rounded_fixed %4))%4;
    byte *compressed_variable_begin = compressed_fixed_begin + rounded_fixed;
    int32 rounded_variable = compressed_variable_size;
    rounded_variable += (4-(rounded_variable%4))%4;

    AssertAlways(header_len + rounded_fixed + rounded_variable == (int32)from.size(),
		 ("Invalid extent data\n"));

    fixeddata.resize(nrecords * type->fixed_record_size);
    unpackAny(fixeddata.begin(),compressed_fixed_begin,
	      compressed_fixed_mode,
	      nrecords * type->fixed_record_size,
	      compressed_fixed_size);
    variabledata.resize(variable_size);
    AssertAlways(variable_size >= 4,("error unpacking, invalid variable size\n"));
    *(int32 *)variabledata.begin() = 0;
    unpackAny(variabledata.begin()+4, compressed_variable_begin,
	      compressed_variable_mode,
	      variable_size-4, compressed_variable_size);
    uint32_t bjhash = 0;
    if (dataseries_enable_postuncompress_check) {
	bjhash = BobJenkinsHash(1972,fixeddata.begin(),fixeddata.size());
	bjhash = BobJenkinsHash(bjhash,variabledata.begin(),variabledata.size());
    }
    std::vector<int32> variable_sizes;
    variable_sizes.reserve(variable_sizes_batch_size);
    byte *endvarpos = variabledata.begin() + variabledata.size();
    for(byte *curvarpos = &variabledata[4];curvarpos != endvarpos;) {
	int32 size = *(int32 *)curvarpos;
	variable_sizes.push_back(size);
	if ((int)variable_sizes.size() == variable_sizes_batch_size) {
	    if (dataseries_enable_postuncompress_check) {
		bjhash = BobJenkinsHash(bjhash,&(variable_sizes[0]),4*variable_sizes_batch_size);
	    }
	    variable_sizes.resize(0);
	}
	if (fix_endianness) {
	    size = Extent::flip4bytes(size);
	    *(int32 *)curvarpos = size;
	}
	curvarpos += 4 + Variable32Field::roundupSize(size);
	AssertAlways(curvarpos <= endvarpos,("internal error\n"));
    }
    if (dataseries_enable_postuncompress_check) {
	bjhash = BobJenkinsHash(bjhash,&(variable_sizes[0]),4*variable_sizes.size());
    }
    variable_sizes.resize(0);
    AssertAlways(dataseries_enable_postuncompress_check == false || *(int32 *)(from.begin() + 5*4) == (int32)bjhash,
		 ("final partially unpacked hash check failed\n"));
    
    std::vector<ExtentType::pack_self_relativeT> psr_copy = type->pack_self_relative;
    for(unsigned int j=0;j<type->pack_self_relative.size();++j) {
	AssertAlways(psr_copy[j].double_prev_v == 0 &&
		     psr_copy[j].int32_prev_v == 0 &&
		     psr_copy[j].int64_prev_v == 0,("internal"));
    }
    TIME_UNPACKING(Clock::Tdbl time_postuc = Clock::tod());
    int record_count = 0;
    // 2004-09-26: each one of these is worth a small speedup, 0.5-1%
    // or so I'd guess, while not inherently worth it, the fix was
    // made when profiling accidentally with the debugging library, 
    // and there is no point in removing the fix.
    const unsigned type_variable32_field_columns_size = type->variable32_field_columns.size();
    const unsigned type_pack_scale_size = type->pack_scale.size();
    const unsigned type_pack_self_relative_size = psr_copy.size();
    const unsigned type_pack_other_relative_size = type->pack_other_relative.size();
    for(ExtentSeries::iterator pos(this);pos.morerecords();++pos) {
	++record_count;
	if (fix_endianness) {
	    for(unsigned int j=0;j<type->field_info.size();j++) {
		switch(type->field_info[j].type)
		    {
		    case ExtentType::ft_bool: 
		    case ExtentType::ft_byte:
			break;
		    case ExtentType::ft_int32:
		    case ExtentType::ft_variable32:
			Extent::flip4bytes(pos.record_start() + type->field_info[j].offset);
			break;
		    case ExtentType::ft_int64:
		    case ExtentType::ft_double:
			Extent::flip8bytes(pos.record_start() + type->field_info[j].offset);
			break;
		    default:
			AssertFatal(("unknown field type %d for fix_endianness\n",type->field_info[j].type));
			break;
		    }
	    }
	}
	// check variable sized fields ...
	if (dataseries_enable_unpack_variable32_check) {
	    for(unsigned int j=0;j<type_variable32_field_columns_size;j++) {
		int field = type->variable32_field_columns[j];
		int32 offset = type->field_info[field].offset;
		int32 varoffset = Variable32Field::getVarOffset(pos.record_start(),
								offset);
		// now check with the standard verification routine
		Variable32Field::selfcheck(variabledata,varoffset);
	    }
	}     
	// Unpacking is done in the reverse order as packing.

	// unpack scaled fields ...
	for(unsigned int j=0;j<type_pack_scale_size;++j) {
	    int field = type->pack_scale[j].field_num;
	    AssertAlways(type->field_info[field].type == ExtentType::ft_double,
			 ("internal error, scaled only supported for ft_double\n"));
	    int offset = type->field_info[field].offset;
	    double scale = type->pack_scale[j].scale;
	    double v = *(double *)(pos.record_start() + offset);
	    *(double *)(pos.record_start() + offset) = v * scale;
	}

	// unpack self-relative fields ...
	for(unsigned int j=0;j<type_pack_self_relative_size;++j) {
	    int field = psr_copy[j].field_num;
	    int offset = type->field_info[field].offset;
	    switch(type->field_info[field].type) 
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
		    AssertFatal(("unimplemented\n"));
		}
	}
	// unpack other-relative fields ...
	for(unsigned int j=0;j<type_pack_other_relative_size;++j) {
	    // 2004-09-26 I cannot believe this is worth a 1% speedup (pulling out v).
	    const ExtentType::pack_other_relativeT &v = type->pack_other_relative[j];
	    int field = v.field_num;
	    int base_field = v.base_field_num;
	    int field_offset = type->field_info[field].offset;
	    switch(type->field_info[field].type)
		{
		case ExtentType::ft_double: {
		    double v = *(double *)(pos.record_start() + field_offset);
		    double base_v = *(double *)(pos.record_start() + type->field_info[base_field].offset);
		    *(double *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		case ExtentType::ft_int32: {
		    int32 v = *(int32 *)(pos.record_start() + field_offset);
		    int32 base_v = *(int32 *)(pos.record_start() + type->field_info[base_field].offset);
		    *(int32 *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		case ExtentType::ft_int64: {
		    int64 v = *(int64 *)(pos.record_start() + field_offset);
		    int64 base_v = *(int64 *)(pos.record_start() + type->field_info[base_field].offset);
		    *(int64 *)(pos.record_start() + field_offset) = v + base_v;
		}
		break;
		default:
		    AssertFatal(("Internal error\n"));
		}
	}
    }	
    AssertAlways(record_count == nrecords,("internal error\n"));
    TIME_UNPACKING(Clock::Tdbl time_done = Clock::tod();
    printf("%d records, unpackcheck %.6g; uncompress %.6g; unpack %.6g\n",
	   nrecords,
	   time_upc - time_start,
	   time_postuc - time_upc,
	   time_done - time_postuc));
}

bool
Extent::checkedPread(int fd, off64_t offset, byte *into, int amount, bool eof_ok)
{
    ssize_t ret = pread64(fd,into,amount,offset);
    AssertAlways(ret != -1,("error reading %d bytes: %s\n",amount,
			    strerror(errno)));
    if (ret == 0 && eof_ok) {
	return false;
    }
    AssertAlways(ret == amount,("partial read %d of %d bytes: %s\n",
				ret,amount,strerror(errno)));
    return true;
}

bool
Extent::preadExtent(int fd, off64_t &offset, Extent::ByteArray &into, bool need_bitflip)
{
    int prefix_size = 6*4 + 4*1;
    into.resize(prefix_size);
    if (checkedPread(fd,offset,into.begin(),prefix_size, true) == false) {
	into.resize(0);
	return false;
    }
    offset += prefix_size;
    byte *l = into.begin();
    int32 compressed_fixed = *(int32 *)l; l += 4;
    int32 compressed_variable = *(int32 *)l; l += 4;
    int32 typenamelen = into[6*4+2];
    if (need_bitflip) {
	compressed_fixed = flip4bytes(compressed_fixed);
	compressed_variable = flip4bytes(compressed_variable);
    }
    if (compressed_fixed == -1) {
	DataSeriesSink::verifyTail(into.begin(), need_bitflip,"*unknown*");
	return false;
    }
    AssertAlways(compressed_fixed >= 0 && compressed_variable >= 0 &&
		 typenamelen >= 0,("Error reading extent\n"));
    int extentsize = prefix_size+typenamelen;
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_fixed;
    extentsize += (4 - extentsize % 4) % 4;
    extentsize += compressed_variable;
    extentsize += (4 - extentsize % 4) % 4;
    into.resize(extentsize);
    checkedPread(fd, offset, into.begin() + prefix_size, 
		 extentsize - prefix_size);
    offset += extentsize - prefix_size;
    return true;
}

void
Extent::run_flip4bytes(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = Extent::flip4bytes(buf[i]);
    }
}
