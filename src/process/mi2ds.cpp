/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

int
main(int argc, char *argv[])
{
    if (argc > 1 && strncmp(argv[1],"-h",2)==0) {
	fprintf(stderr,"Usage: %s [-t max-io-time-seconds, default 120 seconds]\n"
		"    [--marker, default off] [-o output-ds-file, default stdout]\n"
		"    [mi-file, default stdin]\n",argv[0]);
	exit(1);
    }
    string outputname = "-";
    string inputname = "-";
    double max_io_time = 120;
    bool marker = false;
    for(unsigned int i = 1; i < argc; ++i) {
	if (strcmp(argv[i],"-o") == 0) {
	    ++i;
	    AssertAlways(i < argc,("-o missing argument\n"));
	    outputname = argv[i];
	} else if (strcmp(argv[i],"-t")==0) {
	    ++i;
	    AssertAlways(i < argc,("-t missing argument\n"));
	    max_io_time = atof(argv[i]);
	    AssertAlways(max_io_time > 0,("invalid -t argument\n"));
	} else if (strcmp(argv[i],"--marker")==0) {
	    marker = true;
	} else {
	    AssertAlways(argv[i][0] != '-',
			 ("Unknown argument %s\n",argv[i]));
	    AssertAlways(i+1 == argc,("mi-file should be last argument\n"));
	    inputname = argv[i];
	}
    }
    if (inputname != "-") {
	AsssertAlways(freopen64(inputname.c_str(), "r", stdin) != NULL,
		      ("can't open %s for input\n", inputname.c_str()));
    }
    mi_init(inputname);
    mi_check_file();
    
}
