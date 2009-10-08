/*
 * Reducer.h
 *
 *  Created on: Apr 24, 2009
 *      Author: shirant
 */

#ifndef REDUCER_H_
#define REDUCER_H_

#include <vector>

#include "Record.h"

class Reducer {
public:
    Reducer();
    virtual ~Reducer();

    virtual void reduce(const std::vector<Record*> &inputRecords, Record *outputRecord) = 0;
};

#endif /* REDUCER_H_ */
