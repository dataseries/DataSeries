INCLUDE(LintelCMakeUtil)

#
# cmake sometimes run this twice, so use -f to avoid an error.   later,
# we should figure out why this is running wtice.
ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib
    COMMAND ln 
    ARGS -sf ${CMAKE_CURRENT_SOURCE_DIR}/../tr/references.bib
      ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tr-fig
    COMMAND ln 
    ARGS -s ${CMAKE_CURRENT_SOURCE_DIR}/../tr/fig
      ${CMAKE_CURRENT_BINARY_DIR}/tr-fig)

SET(dataseries-osr-2008_EXTRA_DEPENDS 
    ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib
    ${CMAKE_CURRENT_BINARY_DIR}/tr-fig
)

IF(LINTEL_LATEX_REBUILD_ENABLED)
    LINTEL_LATEX(dataseries-osr-2008)
ENDIF(LINTEL_LATEX_REBUILD_ENABLED)
