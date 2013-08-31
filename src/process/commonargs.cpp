// -*-C++-*-
/*
  (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DataSeries/commonargs.hpp>
#include <iostream>
using boost::format;

// Specifies the different options for dealing with a specified compression alg.
enum FlagType {
    ENABLE, DISABLE, COMPRESS
};

// Handles command-line arguments following a flag that specifies what to do
// with those algorithms by setting the values in the commonArgs instance
// accordingly.  (i.e. after getPackingArgs() sees "--compress" in argv,
// flag_handler takes over and parses which algorithms to compress)
void flag_handler(int* argc, char* argv[], int& cur_arg, int& num_munged_args, 
                  char* arg, commonPackingArgs* commonArgs, FlagType flagType) {
    // Go through the command-line arguments and deal with every argument until
    // hitting another one that begins with --.
    for (++cur_arg ; cur_arg < *argc ; ++cur_arg) {
        ++num_munged_args;
        arg = argv[cur_arg];
                
        if (arg[0] == '-' && arg[1] == '-') {
            // Since all these variables are references to the ones in 
            // getPackingArgs, but we just determined that this next argument
            // is not going to be handled in flag_handler, we need to make
            // sure that getPackingArgs will actually handle this argument.
            --num_munged_args;
            --cur_arg;
            break;
        }
        
        // flag_handler only expects to deal with argument sequences where each
        // argument is the name of a compression algorithm, but it expects that
        // getPackingArgs might know how to handle other sorts of arguments, so
        // we need to retain those non-algorithm arguments for someone else to
        // deal with.
        bool isAnAlg = false;
        // Check the current argument against names of known compression 
        // algorithms.  If it is a compression alg, set the compress_modes
        // appropriately.
        for (int i = 0; i < Extent::num_comp_algs; ++i) {
            if (strcmp(Extent::compression_algs[i].name, arg) == 0) {
                switch(flagType) {
                    case ENABLE:
                        commonArgs -> compress_modes |= 
                                Extent::compression_algs[i].compress_flag;
                        break;
                    case DISABLE:
                        commonArgs -> compress_modes &= 
                                ~Extent::compression_algs[i].compress_flag;
                        break;
                    case COMPRESS:
                        commonArgs -> compress_modes = 
                                Extent::compression_algs[i].compress_flag;
                        break;
                    default:
                        // The flag type was not one of the elements of the enum
                        break;
                }
                isAnAlg = true;
                break;
            }
        }
        // Wasn't a compression algorithm ---  make sure that the argument isn't counted as one
        if (!isAnAlg) {
            --num_munged_args;
        }
    }
}

// Handles all command-line arguments beginning with --.  Trailing args that
// don't begin with -- are left untouched, but shifted to the beginning of
// argv.  Does read all the way through argv, though.
void
getPackingArgs(int *argc, char *argv[], commonPackingArgs *commonArgs) {
    int cur_arg = 1;

    // Keep count of how many elements of argv are munged by this function
    int num_munged_args = 0;
    // Looping over the command line arguments
    for (; cur_arg < *argc; ++cur_arg) {
        char *arg = argv[cur_arg];

        // If the argument isn't designated as an argument, ignore it
        if (!(arg[0] == '-' && arg[1] == '-')) {
            continue;
        }
        // If the third character is the null character, we're done
        if (arg[2] == '\0') {
            break;
        }
        ++num_munged_args;

        // Invoke the flag_handler to handle setting the compression flags
        // appropriately.
        if (strcmp(arg, "--enable") == 0) {
            flag_handler(argc, argv, cur_arg, num_munged_args, arg, 
                         commonArgs, ENABLE);
        } else if (strcmp(arg, "--disable") == 0) {
            flag_handler(argc, argv, cur_arg, num_munged_args, arg, 
                         commonArgs, DISABLE);
        } else if (strcmp(arg, "--compress") == 0) {
            flag_handler(argc, argv, cur_arg, num_munged_args, arg,
                         commonArgs, COMPRESS);

            // Note that not all compression algorithms support specifying the
            // compression level.
        } else if (strncmp(argv[cur_arg],"--compress-level=",17) == 0) {
            commonArgs->compress_level = atoi(argv[cur_arg]+17);
            INVARIANT(commonArgs->compress_level > 0 
                      && commonArgs->compress_level < 10,
                      format("compression level %d (%s) invalid, should be 1..9")
                      % commonArgs->compress_level % argv[cur_arg]);
        } else if (strncmp(argv[cur_arg],"--extent-size=",14) == 0) {
            commonArgs->extent_size = atoi(argv[cur_arg]+14);
            INVARIANT(commonArgs->extent_size >= 1024,
                      format("extent size %d (%s), < 1024 doesn't make sense")
                      % commonArgs->extent_size % argv[cur_arg]);
        } else {
            // Ignore unrecognized arguments, but keep looking for more...
            // Caution: if a recognized argument is found following this one
            // the unrecognized argument will be removed instead and not be dealt
            // with later by the outer program. Consequently, the recognized argument
            // will be handled but not removed from the argument array. 
            --num_munged_args;
            continue; 
        }
    }
    
    // Shift all the non-munged elements of argv to the beginning of argv,
    // and adjust argc appropriately so that the outer program can handle those
    // non-munged arguments.
    for (int i = num_munged_args + 1; i < *argc; ++i) {
        argv[i - num_munged_args] = argv[i];
    }
    *argc -= (num_munged_args);

    if (commonArgs->extent_size < 0) {
        // the analysis work in the paper shows that 64-96k is the
        // peak decompression rate, with a sacrifice of a slight
        // amount from the maximum compression ratio.  Compression
        // ratio flattens out somewhere around 512k, but if we are not
        // using bz2, then the goal is likely speed rather than
        // compression, so default to a smaller extent size.  For bz2,
        // we are going for maximal compression, which is flat past
        // 8MB.
        commonArgs->extent_size = 64*1024;
        if (commonArgs->compress_modes & 
            Extent::compression_algs[Extent::compress_mode_bz2].compress_flag) {
            commonArgs->extent_size = 16*1024*1024;
        }
    }
}

// If unrecognized arguments are passed this string is returned
// describing the possible options.
const std::string packingOptions() {
    std::string returnStr = "";
    returnStr += "    --{disable, compress, enable} {";
    for (int i = 0; i < Extent::num_comp_algs; ++i) {
        if (i != 0) {
            returnStr += ",";
        }
        returnStr += Extent::compression_algs[i].name;
    }
    returnStr += 
            "} (default enables all --- enable does little on its own)\n"
            "    --compress-level=[0-9] (default 9)\n"
            "    --extent-size=[>=1024] (default 16*1024*1024 if bz2 is "
            "enabled, 64*1024 otherwise)\n";

    return returnStr;
}
