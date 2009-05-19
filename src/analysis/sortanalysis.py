#!/usr/bin/env python

import getopt, sys, os
from experimentation import MeasurementDatabase, Experiment, run, buildCombinations

COMPRESSION_TYPES = ('none', 'lzf', 'gz')

class SortMeasurementDatabase(MeasurementDatabase):
	experimentTableName = "SortExperiments"
	measurementTableName = "SortMeasurements"


def cond(expression, trueVal, falseVal):
	if expression:
		return trueVal
	else:
		return falseVal

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
3) prepareGensort --records=<records> <output file prefix>
       Prepares multiple DS files (various compression algorithms) with the specified number of 100-
       byte records. The records are first generated using gensort and stored in a file with the
       specified prefix and a suffix of .bin. The suffixes of the DS files are (.none.in, .lzf.in, .gz.in).
4) experiment [--cache] [--iterations=<iterations>] [--tag=<tag>] [--gensort]
     [--extentLimit=<extentLimit>] [--memoryLimit=<memoryLimit>] [--compressTemp] [--compressOutput]
     <input file prefix> <output file>
       Runs the experiment to sort with sortanalysis. The DS files are expected to have the specified
       file prefix and the suffixes: .none.in, .lzf.in, .gz.in. The output file will have the same
       prefix with the suffixes: .none.out, .lzf.out, .gz.out. There are nine combinations in total.
5) createMercuryTables
       Creates the MySQL database tables in which the results are stored.
6) dropMercuryTables
       Deletes the MySQL database tables in which the results are stored.

Examples:
%(command)s prepareText --copies=200 /usr/share/datasets/bible.txt /usr/share/datasets/bible200
%(command)s experiment --cache --iterations=10 --tag="my experiment" "seven" /usr/share/datasets/bible200""" % {'command': sys.argv[0]}

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
		print "=====> Creating DS file '%(outputFilePrefix)s.%(compressionType)s.in' via 'txt2ds --compress-%(compressionType)s'" % \
			{'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType}
		run("txt2ds --compress-%(compressionType)s %(outputFileTxt)s %(outputFilePrefix)s.%(compressionType)s.in" % \
			{'outputFileTxt': outputFileTxt, 'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType})

def prepareGensort(outputFilePrefix, recordCount):
	outputFileBin = outputFilePrefix + ".bin"

	print "=====> Creating binary file '%s' via 'gensort %s'" % (outputFileBin, recordCount)
	run('gensort %s %s' % (recordCount, outputFileBin))
	
	for compressionType in COMPRESSION_TYPES:
		print "=====> Creating DS file '%(outputFilePrefix)s.%(compressionType)s.in' via 'gensort2ds --compress-%(compressionType)s'" % \
			{'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType}
		run("gensort2ds --compress-%(compressionType)s %(outputFileBin)s %(outputFilePrefix)s.%(compressionType)s.in" % \
			{'outputFileBin': outputFileBin, 'outputFilePrefix': outputFilePrefix, 'compressionType': compressionType})
			
	run('rm "%s"' % outputFileBin)

def experiment(inputFilePrefix, outputFile, cache, iterations, tag, gensort, options): # outputFile is temporary
	experiment = Experiment(database, tag)
	print "Starting experiment: %s" % repr(experiment)
	
	if not gensort:
		inputFileTxt = inputFilePrefix + ".txt"
		commands = ["sort %s > %s" % (inputFileTxt, outputFile)]
	else:
		commands = []
	
	optionCombinations = buildCombinations(list(options.iteritems()))
	
	for compression in COMPRESSION_TYPES:
		for optionCombination in optionCombinations:
			commands.append("sortanalysis %(fixedWidth)s --inputFile=\"%(inputFilePrefix)s.%(compression)s.in\" --outputFile=\"%(outputFile)s\" %(optionCombination)s" % {
				'compression': compression,
				'inputFilePrefix': inputFilePrefix,
				'outputFile': outputFile,
				'fixedWidth': cond(gensort, '--fixedWidth', ''),
				'optionCombination': optionCombination
				})
		
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

if __name__ == "__main__":
	if len(sys.argv) == 1:
		print "ERROR: no command specified"
		exitUsage(1)
	
	command = sys.argv.pop(1)
	
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "", ["copies=", "iterations=", "cache", "tag=", "records=", "gensort", "extentLimit=", "memoryLimit=", "compressTemp", "compressTemp?", "compressOutput", "compressOutput?"])
		opts = dict(opts)
	except getopt.GetoptError, err:
		print "ERROR: %s" % err
		exitUsage(2)
	
	database = MeasurementDatabase("SortExperiments", "SortMeasurements")
	
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
		prepareText(*args)
	elif command == 'prepareGensort':
		if len(args) != 1:
			exitUsage(3)
		args.append(int(opts.get('--records')))
		prepareGensort(*args)
	elif command == 'experiment':
		if len(args) != 2:
			exitUsage(3)
		args.append(opts.pop('--cache', None) is not None)
		args.append(int(opts.pop('--iterations', '1')))
		args.append(opts.pop('--tag', ''))
		args.append(opts.pop('--gensort') is not None)
		args.append(opts)
		database.connect()
		experiment(*args)
		database.close()
	else:
		exitUsage(3)
		