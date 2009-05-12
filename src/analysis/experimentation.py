import MySQLdb
from subprocess import call
import sys	
import os

def runText(command):
	(stdinHandle, stdoutHandle, stderrHandle) = os.popen3(command)
	return (stdoutHandle.read(), stderrHandle.read())

def runLines(command):
	(stdinHandle, stdoutHandle, stderrHandle) = os.popen3(command)
	return (stdoutHandle.readlines(), stderrHandle.readlines())

def run(command):
	os.system(command)

class MeasurementDatabase:
	def __init__(self, experimentTableName, measurementTableName, databaseName="test"):
		self.experimentTableName = experimentTableName
		self.measurementTableName = measurementTableName
		self.databaseName = databaseName # mercury only works with "test" database
		self.lastExperimentId = None
		self.connection = None

	def connect(self):
		self.connection = MySQLdb.connect(db=self.databaseName)
	
	def close(self):
		self.connection.close()
		self.connection = None
	
	def createTables(self):
		cursor = self.connection.cursor()
		cursor.execute("""
			CREATE TABLE %s
			(
				id INT NOT NULL AUTO_INCREMENT,
				tag VARCHAR(256),
				date TIMESTAMP NOT NULL DEFAULT NOW(),
				PRIMARY KEY (id)
			);
			""" % self.experimentTableName)
		cursor.execute("""
			CREATE TABLE %s
			(
				id INT NOT NULL AUTO_INCREMENT,
				tag VARCHAR(256),
				experimentId INT NOT NULL,
				elapsed FLOAT,
				user FLOAT,
				system FLOAT,
				cpu FLOAT,
				date TIMESTAMP NOT NULL DEFAULT NOW(),
				PRIMARY KEY (id),
				CONSTRAINT FOREIGN KEY fkMeasurementToExperint (experimentId) REFERENCES %s(id) ON DELETE CASCADE
			);
			""" % (self.measurementTableName, self.experimentTableName,))
		cursor.close()
		
	def dropTables(self):
		cursor = self.connection.cursor()
		cursor.execute("DROP TABLE %s" % self.measurementTableName)
		cursor.execute("DROP TABLE %s" % self.experimentTableName)
		cursor.close()
		
	def addExperiment(self, tag=None):
		"""Adds a new experiment to the database and returns it's ID. The ID can then be used when
		calling addMeasurement."""
		cursor = self.connection.cursor()
		cursor.execute("""INSERT INTO %s (tag) VALUES ("%s");""" % (self.experimentTableName, tag,))
		self.lastExperimentId = cursor.lastrowid
		cursor.close()
		return self.lastExperimentId

	def addMeasurement(self, experimentId, tag, elapsed=-1.0, user=-1.0, system=-1.0, cpu=-1.0):
		if experimentId is None:
			experimentId = self.lastExperimentId
		cursor = self.connection.cursor()
		cursor.execute("""
			INSERT INTO %s (experimentId, tag, elapsed, user, system, cpu)
			VALUES (%s, "%s", %s, %s, %s, %s);
			""" % (self.measurementTableName, experimentId, tag, elapsed, user, system, cpu,))
		cursor.close()

class Experiment:
	def __init__(self, database, tag=None, experimentId=None):
		self.tag = tag
		self.experimentId = experimentId
		self.database = database

		if self.experimentId is None:
			self.experimentId = database.addExperiment(tag)
		
	def measure(self, command, tag=None):
		(stdoutLines, stderrLines) = runLines('/usr/bin/time -f "%e %U %S %P" ' + command)
		line = stderrLines[-1]
		components = line.split()
		measurements = {'elapsed': float(components[0]), 'user': float(components[1]), 'system': float(components[2]), 'cpu': float(components[3][0:-1])}
		self.database.addMeasurement(self.experimentId, tag, **measurements)
	
	def __repr__(self):
		return """Experiment(id=%s, tag="%s")""" % (self.experimentId, self.tag)
