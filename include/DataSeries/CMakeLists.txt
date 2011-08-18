#
# (c) Copyright 2007-2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake description for the include/DataSeries directory

SET(INCLUDE_FILES
	ByteField.hpp
	DataSeriesFile.hpp
        DataSeriesSink.hpp
        DataSeriesSource.hpp
	DSExpr.hpp
	DStoTextModule.hpp
	Extent.hpp
	ExtentField.hpp
	ExtentSeries.hpp
	ExtentType.hpp
	Field.hpp
	FixedField.hpp
	FixedWidthField.hpp
	GeneralField.hpp
	GroupByModule.hpp
        IExtentSink.hpp
	IndexSourceModule.hpp
	Int32Field.hpp
	Int64Field.hpp
	Int64TimeField.hpp
	MinMaxIndexModule.hpp
	DataSeriesModule.hpp
	PrefetchBufferModule.hpp
        RotatingFileSink.hpp
	RowAnalysisModule.hpp
	SequenceModule.hpp
	TFixedField.hpp
	TypeIndexModule.hpp
	TypeFilterModule.hpp
	commonargs.hpp
	cryptutil.hpp
)

SET(DATASERIES_BZIP2_ENABLED ${BZIP2_ENABLED})
SET(DATASERIES_LZO_ENABLED ${LZO_ENABLED})
SET(DATASERIES_CRYPTO_ENABLED ${CRYPTO_ENABLED})

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/Config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/Config.hpp)

################################## CONDITIONAL BITS

IF(BOOST_FOREACH_ENABLED)
    LIST(APPEND INCLUDE_FILES SortedIndexModule.hpp )
ENDIF(BOOST_FOREACH_ENABLED)

################################## INSTALL

INSTALL(FILES ${INCLUDE_FILES} 
        ${CMAKE_CURRENT_BINARY_DIR}/Config.hpp
	DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataSeries)

