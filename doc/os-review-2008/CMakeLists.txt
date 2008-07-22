INCLUDE(LintelCMakeUtil)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib
    COMMAND ln 
    ARGS -sf ${CMAKE_CURRENT_SOURCE_DIR}/../tr/references.bib
      ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tr-fig
    COMMAND ln 
    ARGS -sf ${CMAKE_CURRENT_SOURCE_DIR}/../tr/fig
      ${CMAKE_CURRENT_BINARY_DIR}/tr-fig)

SET(dataseries-osr-2008_EXTRA_DEPENDS 
    ${CMAKE_CURRENT_BINARY_DIR}/tr-references.bib
    ${CMAKE_CURRENT_BINARY_DIR}/tr-fig
)

LINTEL_LATEX(dataseries-osr-2008)
