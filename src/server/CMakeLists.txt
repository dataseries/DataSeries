SET(LINTEL_FLOCK_FIND_REQUIRED ON)
LINTEL_FIND_PROGRAM(LINTEL_FLOCK lintel-flock)
SET(thrift_DataSeriesServer_gen_cpp
        ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/DataSeriesServer_constants.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/DataSeriesServer_types.cpp
	${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/DataSeriesServer.cpp
)
SET(thrift_DataSeriesServer_gen_hpp
        ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/DataSeriesServer_constants.h
        ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/DataSeriesServer_types.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT ${thrift_DataSeriesServer_gen_cpp} ${thrift_DataSeriesServer_gen_hpp}
    COMMAND
        ${LINTEL_FLOCK_PATH}
    ARGS
        --filename=DataSeriesServer.lock
        --command='${THRIFT_PATH} -I ${CMAKE_INSTALL_PREFIX}/include --gen cpp --gen perl ${CMAKE_CURRENT_SOURCE_DIR}/DataSeriesServer.thrift'

    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/DataSeriesServer.thrift
        ${THRIFT_PATH}
        ${SIMREAL_AUTOREGISTER}
)

INCLUDE_DIRECTORIES(${THRIFT_INCLUDES} ${CMAKE_CURRENT_BINARY_DIR})

DATASERIES_PROGRAM(server ${thrift_DataSeriesServer_gen_cpp})
TARGET_LINK_LIBRARIES(server ${DATASERIES_LIBRARIES} ${THRIFT_LIBRARIES})
