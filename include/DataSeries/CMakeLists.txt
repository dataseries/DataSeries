#
# (c) Copyright 2007-2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake description for the include/DataSeries directory

SET(INCLUDE_FILES
	DataSeriesFile.H
	DSExpr.hpp
	DStoTextModule.H
	Extent.H
	ExtentField.H
	ExtentSeries.H
	ExtentType.H
	Field.hpp
	FixedField.hpp
	GeneralField.H
	GroupByModule.H
	IndexSourceModule.H
	Int64Field.hpp
	Int64TimeField.hpp
	MinMaxIndexModule.H
	DataSeriesModule.H
	PrefetchBufferModule.H
	RowAnalysisModule.H
	SequenceModule.H
	TypeIndexModule.H
	commonargs.H
	cryptutil.H
)

SET(TRANSITIONAL_INCLUDE_FILES)

################################## INSTALL

INSTALL(FILES ${INCLUDE_FILES} ${TRANSITIONAL_INCLUDE_FILES}
	DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataSeries)

