#!/usr/bin/env python

import getopt, sys, os
from experimentation import MeasurementDatabase, Experiment, run

COMPRESSION_TYPES = ('none', 'lzf', 'gz')


class SortMeasurementDatabase(MeasurementDatabase):
	experimentTableName = "SortExperiments"
	measurementTableName = "SortMeasurements"


def printUsage():
	print """Usage: %(command)s <command>
Various commands are supported:
1) prepareText [--copies=<copies>] <input text file> <output file prefix>
       Prepares a text file and multiple DS files (various compression algorithms). The output files
       are named automatically by adding suffixes (.txt, .none.in, .lzf.in, .gz.in).
2) experimentText [--cache] [--iterations=<iterations>] [--tag=<tag>] <input file prefix> <output file>
       Runs the experiment to compare standard sort with sortanalysis. The text and DS files
       are expected to have the specified file prefix and the suffixes: .txt, .none.in, .lzf.in, .gz.in.
       If --cache is specified, an additional iteration is performed first without recording the
       results. If --cache is not specified, the cache is cleared first by writing a large file to /tmp.
3) prepareBinary --records=<records> <output file prefix>
       Prepares multiple DS files (various compression algorithms) with the specified number of 100-
       byte records. The records are first generated using gensort and stored in a file with the
       specified prefix and a suffix of .bin. The suffixes of the DS files are (.none.in, .lzf.in, .gz.in).
4) experimentBinary [--cache] [--iterations=<iterations>] [--tag=<tag>] <input file prefix> <output file>
       Runs the experiment to sort with sortanalysis. The DS files are expected to have the specified
       file prefix and the suffixes: .none.in, .lzf.in, .gz.in. The output file will have the same
       prefix with the suffixes: .none.out, .lzf.out, .gz.out. There are nine combinations in total.
5) createMercuryTables
       Creates the MySQL database tables in which the results are stored.
6) dropMercuryTables
       Deletes the MySQL database tables in which the results are stored.

Examples:
%(command)s prepareText --copies=200 /usr/share/datasets/bible.txt /usr/share/datasets/bible200
%(command)s experimentText --cache --iterations=10 --tag="my experiment" "seven" /usr/share/datasets/bible200""" % {'command': sys.argv[0]}

def exitUsage(retvalue):
	printUsage()
	sys.exit(retvalue)

def prepareText(inputTextFile, outputFilePrefix, copies):
	outputFileTxt = outputFilePrefix + ".txt"

	assert(outputFileTxt != inputTextFile)

	print "=====> Creating text file '%s'" % outputFileTxt
	run('cat %s > %s' % (inputTextFile, outputFileTxt))
	for i in range(copies - 1):
		run('cat %s >> %s' % (inputTextFile, outputFileTxt))
	
	for compressionType in COMPRESSION_TYPES:	
		print "=====> Creating DS file '%(outputFilePrefix)s.%(compressionType)s.in' via 'txt2ds --compress-%(compressionType)s'" %
			{'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType}
		run("txt2ds --compress-%(compressionType)s %(outputFileTxt)s %(outputFilePrefix)s.%(compressionType)s.in" %
			{'outputFileTxt': outputFileTxt, 'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType})

def prepareBinary(outputFilePrefix, recordCount):
	outputFileBin = outputFilePrefix + ".bin"

	print "=====> Creating binary file '%s' via 'gensort %s'" % (outputFileBin, recordCount)
	run('gensort %s %s' % (recordCount, outputFileBin))
	
	for compressionType in COMPRESSION_TYPES:
		print "=====> Creating DS file '%(outputFilePrefix)s.%(compressionType)s.in' via 'bin2ds --compress-%(compressionType)s'" %
			{'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType}
		run("bin2ds --compress-%(compressionType)s %(outputFileBin)s %(outputFilePrefix)s.%(compressionType)s.in" %
			{'outputFileBin': outputFileBin, 'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType})

def experiment(inputFilePrefix, outputFile, cache, iterations, tag, binary): # outputFile is temporary
	experiment = Experiment(database, tag)
	print "Starting experiment: %s" % repr(experiment)
	
	# total of nine or ten different experiments:
	# 1: {(sort, text, text)} (only if binary is False)
	# 9: {sortanalysis} x {none, lzf, gz} x {none, lzf, gz}

	if not binary:
		inputFileTxt = inputFilePrefix + ".txt"
		commands = ["sort %s > %s" % (inputFileTxt, outputFile)]
	
	for inputType in COMPRESSION_TYPES:
		for outputType in COMPRESSION_TYPES:
			commands.append("sortanalysis --compression-%(outputType)s %(recordFlag)s --inputFile=\"%(inputFilePrefix)s.%(inputType)s.in\" --outputFile=\"%(outputFile)s\"" % {
				'inputType': inputType,
				'outputType': outputType,
				'inputFilePrefix': inputFilePrefix,
				'outputFile': outputFile
				'recordFlag': ('--binary' if binary else '--text')})
		
	if cache:
		iterations += 1
	
	for command in commands:
		for i in range(iterations):
		
			print command
			if i == 0 and cache:
				run(command)
			else:
				if not cache: # if caching is not desired we must clear the buffer cache
					run("sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'")
				experiment.measure(command, tag=command)

def experimentText(*args):
	args.append(False)
	experiment(*args)

def experimentBinary(*args):
	args.append(True)
	experiment(*args)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		print "ERROR: no command specified"
		exitUsage(1)
	
	command = sys.argv.pop(1)
	
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "", ["copies=", "iterations=", "cache", "tag="])
		opts = dict(opts)
	except getopt.GetoptError, err:
		print "ERROR: %s" % err
		exitUsage(2)
	
	database = MeasurementDatabase("GrepExperiments", "GrepMeasurements")
	
	if command == 'createMercuryTables':
		database.connect()
		database.createTables()
		database.close()
	elif command == 'dropMercuryTables':
		database.connect()
		database.dropTables()
		database.close()
	elif command == 'prepareText':
		if len(args) != 2:
			exitUsage(3)
		args.append(int(opts.get('--copies', '1')))
		prepare(*args)
	elif command == 'prepareBinary':
		if len(args) != 1:
			exitUsage(3)
		args.append(int(opts.get('--records')))
		prepare(*args)
	elif command == 'experimentText':
		if len(args) != 2:
			exitUsage(3)
		args.append(opts.get('--cache') is not None)
		args.append(int(opts.get('--iterations', '1')))
		args.append(opts.get('--tag'))
		args.append(int(opts.get('--memory', '2000000000')))
		args.append(int(opts.get('--maxExtent', '2000000')))
		args.append(False)
		
		database.connect()
		experimentText(*args)
		database.close()
	elif command == 'experimentBinary':
		if len(args) != 2:
			exitUsage(3)
		args.append(opts.get('--cache') is not None)
		args.append(int(opts.get('--iterations', '1')))
		args.append(opts.get('--tag'))
		args.append(int(opts.get('--memory', '2000000000')))
		args.append(int(opts.get('--maxExtent', '2000000')))
		args.append(True)
		
		database.connect()
		experimentBinary(*args)
		database.close()
	else:
		exitUsage(3)
		