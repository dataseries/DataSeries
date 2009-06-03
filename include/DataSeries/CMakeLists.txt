#
# (c) Copyright 2007-2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake description for the include/DataSeries directory

SET(INCLUDE_FILES
	DataSeriesFile.hpp
	DSExpr.hpp
	DStoTextModule.hpp
	Extent.hpp
	ExtentField.hpp
	ExtentSeries.hpp
	ExtentType.hpp
	Field.hpp
	FixedField.hpp
	GeneralField.hpp
	GroupByModule.hpp
	IndexSourceModule.hpp
	Int32Field.hpp
	Int64Field.hpp
	Int64TimeField.hpp
	MinMaxIndexModule.hpp
	SortedIndexModule.hpp
	DataSeriesModule.hpp
	PrefetchBufferModule.hpp
	RowAnalysisModule.hpp
	SequenceModule.hpp
	TFixedField.hpp
	TypeIndexModule.hpp
	commonargs.hpp
	cryptutil.hpp
)

SET(TRANSITIONAL_INCLUDE_FILES
	DataSeriesFile.H
	DStoTextModule.H
	Extent.H
	ExtentField.H
	ExtentSeries.H
	ExtentType.H
	GeneralField.H
	GroupByModule.H
	IndexSourceModule.H
	MinMaxIndexModule.H
	DataSeriesModule.H
	PrefetchBufferModule.H
	RowAnalysisModule.H
	SequenceModule.H
	TypeIndexModule.H
	commonargs.H
	cryptutil.H
)

SET(DATASERIES_BZIP2_ENABLED ${BZIP2_ENABLED})
SET(DATASERIES_LZO_ENABLED ${LZO_ENABLED})
SET(DATASERIES_CRYPTO_ENABLED ${CRYPTO_ENABLED})

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/Config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/Config.hpp)

################################## INSTALL

INSTALL(FILES ${INCLUDE_FILES} ${TRANSITIONAL_INCLUDE_FILES}
        ${CMAKE_CURRENT_BINARY_DIR}/Config.hpp
	DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataSeries)

