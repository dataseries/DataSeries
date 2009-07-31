/*
 * ExtentCollection.cpp
 *
 *  Created on: Apr 28, 2009
 *      Author: shirant
 */

#include "ExtentCollection.h"

ExtentCollection::ExtentCollection() : extents(NULL) {
}

ExtentCollection::~ExtentCollection() {
}

void ExtentCollection::setExtents(const std::vector<Extent*> extents) {
    this->extents = extents;
}
