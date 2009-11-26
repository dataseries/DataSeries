INCLUDE(LintelCMakeUtil)

IF(BUILD_OSR_2008)
    SET(dataseries-osr-2008_LINTEL_LATEX_ARGS
        --bib ${DataSeries_SOURCE_DIR}/doc/tr
        --error-on-warnings
    )
    
    SET(dataseries-osr-2008_EXTRA_DEPENDS 
        ${DataSeries_SOURCE_DIR}/doc/tr/references.bib
    )

    LINTEL_LATEX(dataseries-osr-2008)

    INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/dataseries-osr-2008.pdf
            DESTINATION ${CMAKE_INSTALL_PREFIX}/share/doc/DataSeries)

ENDIF(BUILD_OSR_2008)
