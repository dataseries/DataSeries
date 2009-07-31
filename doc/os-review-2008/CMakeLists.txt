INCLUDE(LintelCMakeUtil)

IF(LINTEL_LATEX_REBUILD_ENABLED)
    SET(dataseries-osr-2008_LINTEL_LATEX_ARGS
        --bib ${DataSeries_SOURCE_DIR}/doc/tr
        --error-on-warnings
    )
    
    SET(dataseries-osr-2008_EXTRA_DEPENDS 
        ${DataSeries_SOURCE_DIR}/doc/tr/references.bib
    )

    LINTEL_LATEX(dataseries-osr-2008)
ENDIF(LINTEL_LATEX_REBUILD_ENABLED)
