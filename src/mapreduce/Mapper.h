/*
 * Mapper.h
 *
 *  Created on: Apr 24, 2009
 *      Author: shirant
 */

#ifndef MAPPER_H_
#define MAPPER_H_

#include <string>

#include <DataSeries/DataSeriesModule.hpp>

#include "Record.h"

class Mapper {
public:
    Mapper();
    virtual ~Mapper();

    virtual void map(Record *inputRecord, Record *outputRecord) = 0;

};

#endif /* MAPPER_H_ */
