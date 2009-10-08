/*
 * ExtentCollection.h
 *
 *  Created on: Apr 28, 2009
 *      Author: shirant
 */

#ifndef EXTENTCOLLECTION_H_
#define EXTENTCOLLECTION_H_

#include <vector>

class Extent;

class ExtentCollection {
public:
    ExtentCollection();
    virtual ~ExtentCollection();

    void setExtents(const std::vector<Extent*> &extents);
    void sortRecords(const Field *field);

private:
    std::vector<Extent*> extents;

    std::vector<int> indices; // the order in which the records are to be accessed

    Field *sortField; // one of the elements of fields (or NULL)
};

class FieldCollection {
public:
    FieldCollection();
    virtual ~FieldCollection();

protected:
    std::vector<Field*> fields;
};

#endif /* EXTENTCOLLECTION_H_ */
