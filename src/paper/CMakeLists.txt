#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/paper directory

DATASERIES_PROGRAM(srtstat)
DATASERIES_PROGRAM(srtstat2)
DATASERIES_PROGRAM(srtcsvstat)

IF(AVRO_ENABLED)
   DATASERIES_PROGRAM(wc-ds-avro)
   TARGET_LINK_LIBRARIES(wc-ds-avro ${AVRO_LIBRARIES})
ENDIF(AVRO_ENABLED)
