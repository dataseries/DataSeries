#!/usr/bin/env python

import getopt, sys, os
from experimentation import MeasurementDatabase, Experiment, run

class FgrepMeasurementDatabase(MeasurementDatabase):
	experimentTableName = "FgrepExperiments"
	measurementTableName = "FgrepMeasurements"


def printUsage():
	print """Usage: %(command)s <command>
Various commands are supported:
1) prepare [--copies=<copies>] <input text file> <output file prefix>
       Prepares a text file and multiple DS files (various compression algorithms). The output files
       are named automatically by adding suffixes (.txt, .none.in, .lzf.in, .gz.in).
2) experiment [--cache] [--iterations=<iterations>] [--tag=<tag>] <pattern> <input file prefix>
       Runs the experiment to compare standard grep with fgrepanalysis. The text and DS files
       are expected for the specified file prefix (.txt, .none.in, .lzf.in, .gz.in).
       
       If --cache is specified, an additional iteration is performed first without recording the
       results. If --cache is not specified, the cache is cleared first by writing a large file to /tmp.
3) create-mercury-tables
       Creates the MySQL database tables in which the results are stored.
4) drop-mercury-tables
       Deletes the MySQL database tables in which the results are stored.

Examples:
%(command)s prepare --copies=200 /usr/share/datasets/fgrepbible.txt /usr/share/datasets/fgrepbible200
%(command)s experiment --cache --iterations=10 --tag="my experiment" "seven" /usr/share/datasets/fgrepbible200""" % {'command': sys.argv[0]}

def exitUsage(retvalue):
	printUsage()
	sys.exit(retvalue)

def prepare(inputTextFile, outputFilePrefix, copies):
	outputFileTxt = outputFilePrefix + ".txt"
	outputFileNone = outputFilePrefix + ".none.in"
	outputFileLzf = outputFilePrefix + ".lzf.in"
	outputFileGz = outputFilePrefix + ".gz.in"

	assert(outputFileTxt != inputTextFile)

	print "=====> Creating text file '%s'" % outputFileTxt
	run('cat %s > %s' % (inputTextFile, outputFileTxt))
	for i in range(copies - 1):
		run('cat %s >> %s' % (inputTextFile, outputFileTxt))
	
	print "=====> Creating DS file '%s' via 'txt2ds --compress-none'" % outputFileNone
	run("txt2ds --compress-none %s %s" % (outputFileTxt, outputFileNone))

	print "=====> Creating DS file '%s' via 'txt2ds --compress-lzf'" % outputFileLzf
	run("txt2ds --compress-lzf %s %s" % (outputFileTxt, outputFileLzf))

	print "=====> Creating DS file '%s' via 'txt2ds --compress-gz'" % outputFileGz
	run("txt2ds --compress-gz %s %s" % (outputFileTxt, outputFileGz))

def experiment(pattern, inputFilePrefix, cache, iterations, tag):
	experiment = Experiment(database, tag)
	print "Starting experiment: %s" % repr(experiment)
	
	# total of five different experiments:
	# 1: {(grep, text)}
	# 3: {DS w/ memcpy} x {none, lzf, gz}
	# 1: {DS wo/ memcpy} x {none}

	inputFileTxt = inputFilePrefix + ".txt"
	inputFileNone = inputFilePrefix + ".none.in"
	inputFileLzf = inputFilePrefix + ".lzf.in"
	inputFileGz = inputFilePrefix + ".gz.in"
	
	commands = (
		"grep --count %s %s" % (pattern, inputFileTxt),

		"fgrepanalysis --count %s %s" % (pattern, inputFileNone),
		"fgrepanalysis --count %s %s" % (pattern, inputFileLzf),
		"fgrepanalysis --count %s %s" % (pattern, inputFileGz),

		"fgrepanalysis --no-memcpy --count %s %s" % (pattern, inputFileNone),
		)

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
		opts, args = getopt.getopt(sys.argv[1:], "", ["copies=", "iterations=", "cache", "tag="])
		opts = dict(opts)
	except getopt.GetoptError, err:
		print "ERROR: %s" % err
		exitUsage(2)
	
	database = MeasurementDatabase("FgrepExperiments", "FgrepMeasurements")
	
	if command == 'create-mercury-tables':
		database.connect()
		database.createTables()
		database.close()
	elif command == 'drop-mercury-tables':
		database.connect()
		database.dropTables()
		database.close()
	elif command == 'prepare':
		if len(args) != 2:
			exitUsage(3)
		args.append(int(opts.get('--copies', '1')))
		prepare(*args)
	elif command == 'experiment':
		if len(args) != 2:
			exitUsage(3)
		args.append(opts.get('--cache') is not None);
		args.append(int(opts.get('--iterations', '1')))
		args.append(opts.get('--tag'))
		
		database.connect()
		experiment(*args)
		database.close()
	else:
		exitUsage(3)
		