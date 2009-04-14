#!/usr/bin/env python

from subprocess import call
import sys	
import os
import re
import getopt

class Measurements:
	def __init__(self, parameters, skipFirst):
		self.parameters = parameters
		self.total = dict([(name, 0.0) for name in parameters])
		self.skip = skipFirst
		self.count = 0
		self.min = dict([(name, float('inf')) for name in parameters])
		self.max = dict([(name, 0.0) for name in parameters])
		
	def record(self, results):
		if self.skip:
			self.skip = False
			return

		for (key, val) in results.iteritems():
			self.total[key] += val
			self.min[key] = min(self.min[key], val)
			self.max[key] = max(self.max[key], val)
		self.count += 1
	
	def mean(self):
		return dict([
			[key, val / self.count] for (key, val) in self.total.iteritems()
			])
	
	def getResultList(self, d):
		return ["%s" % d[key] for key in self.parameters]
		
	def __str__(self):
		return " ".join(self.parameters) + "\n" + \
			"Mean: " + " ".join(self.getResultList(self.mean())) + "\n" + \
			"Min: " + " ".join(self.getResultList(self.min)) + "\n" + \
			"Max: " + " ".join(self.getResultList(self.max))
# TODO-shirant: can we get standard error of mean? this is sample standard deviation / sqrt(number of samples)
# there is an incremental way of diong sample standard deviation


def runText(command):
	(stdinHandle, stdoutHandle, stderrHandle) = os.popen3(command)
	return (stdoutHandle.read(), stderrHandle.read())

def runLines(command):
	(stdinHandle, stdoutHandle, stderrHandle) = os.popen3(command)
	return (stdoutHandle.readlines(), stderrHandle.readlines())

def run(command):
	os.popen(command)

def extractTime(expression, stderrText):
	return expression.search(stderrText).group(2)

def measure(command):
	(stdoutLines, stderrLines) = runLines('/usr/bin/time -f "%e %U %S %P" ' + command)
	line = stderrLines[-1]
	components = line.split()
	return {'elapsed': float(components[0]), 'user': float(components[1]), 'system': float(components[2]), 'cpu': float(components[3][0:-1])}

def printUsage():
	# TODO-shirant: fix documenation of --experiment. copies->iterations?
	print """Usage: %(command)s <command>
Various commands are supported:
1) --prepare [--copies=copies] <input text file> <output text file> <output DS file>
2) --experiment [--iterations=iterations [--skip-first-iteration]] <pattern> <text file> <DS file>
   (grep uses the text file, fgrepanalysis uses the equivalent DS file)

Examples:
%(command)s --prepare --copies=200 /usr/share/datasets/fgrepbible.txt /usr/share/datasets/fgrepbible_200_none.txt /usr/share/datasets/fgrepbible_200_none.in
%(command)s --experiment "seven" /usr/share/datasets/fgrepbible_200_none.txt /usr/share/datasets/fgrepbible_200_none.in""" % {'command': sys.argv[0]}

def exitUsage(retvalue):
	printUsage()
	sys.exit(retvalue)

def prepare(inputTextFile, outputTextFile, outputDsFile, copies):
	print "=====> Creating text file '%s'" % outputTextFile
	runText('cat %s > %s' % (inputTextFile, outputTextFile))
	for i in range(copies - 1):
		runText('cat %s >> %s' % (inputTextFile, outputTextFile))
	
	print "=====> Creating DataSeries file '%s' via 'txt2ds --compress-none'" % outputDsFile
#TODO-shirant - add flag to do these with compression (grep and no-memcpy dataseries uncompressed, vs grep uncompressed and yes-memcpy dataseries compressed); may also consider different compression options (lzf & gz are the most interesting ones)
	run("txt2ds --compress-none %s %s %s" % (inputTextFile, outputDsFile, copies))

def experiment(pattern, textFile, dsFile, iterations, skipFirstIteration):
# TODO-shirant : option for clearing the file cache? clear file cache between each run?
# ask eric about the quick way of throwing out the file cache
# it is interesting to do cold cache/warm cache.

	grepMeasurements = Measurements(['elapsed', 'user', 'system', 'cpu'], skipFirstIteration)
	dsMeasurements = Measurements(['elapsed', 'user', 'system', 'cpu'], skipFirstIteration)
	dsNoMemcpyMeasurements = Measurements(['elapsed', 'user', 'system', 'cpu'], skipFirstIteration)
		
	print "=====> Measuring performance of grep on %s" % textFile
	for i in range(iterations):
		grepMeasurements.record(measure("grep --count %s %s" % (pattern, textFile)))
	print grepMeasurements
	
	print "=====> Measuring performance of 'fgrepanalysis' on %s" % dsFile
	for i in range(iterations):
		dsMeasurements.record(measure("fgrepanalysis --count %s %s" % (pattern, dsFile)))
# TODO-shirant: I had the iterations stuff break on me...		
# =====> Measuring performance of grep on /tmp/bmh.txt
# Traceback (most recent call last):
# 	  File "./fgrepanalysis.py", line 129, in ?
# 		    experiment(*args)
# 				  File "./fgrepanalysis.py", line 97, in experiment
# 					    grepMeasurements.record(measure("grep --count %s %s" % (pattern, textFile)))
# 							  File "./fgrepanalysis.py", line 63, in measure
# 								return {'elapsed': float(components[0]), 'user': float(components[1]), 'system': float(components[2]), 'cpu': float(components[3][0:-1])}
# 							ValueError: invalid literal for float(): ?
# 							[1]    21540 exit 1     ./fgrepanalysis.py --experiment --iterations=20 "needle" /tmp/bmh.txt
							
	print dsMeasurements
	
	print "=====> Measuring performance of 'fgrepanalysis --no-memcpy' on %s" % dsFile
	for i in range(iterations):
		dsNoMemcpyMeasurements.record(measure("fgrepanalysis --count --no-memcpy %s %s" % (pattern, dsFile)))
	print dsNoMemcpyMeasurements

if __name__ == "__main__":
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "", ["prepare", "experiment", "copies=", "iterations=", "skip-first-iteration"])
		opts = dict(opts)
	except getopt.GetoptError, err:
		print "ERROR: %s" % err
		printUsage()
		sys.exit(2)
		
	if '--prepare' in opts:
		if len(args) != 3:
			exitUsage(3)
		args.append(int(opts.get('--copies', '1')))
		prepare(*args)
	elif '--experiment' in opts:
		if len(args) != 3:
			exitUsage(3)
		args.append(int(opts.get('--iterations', '1')))
		args.append(opts.get('--skip-first-iteration') is not None);
		experiment(*args)
	else:
		exitUsage(3)
