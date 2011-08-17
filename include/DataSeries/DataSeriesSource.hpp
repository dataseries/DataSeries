// -*-C++-*-
/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Classes for reading DataSeries files.
*/

#ifndef DATASERIES_SOURCE_H
#define DATASERIES_SOURCE_H

#include <DataSeries/Extent.hpp>

/** \brief Reads Extents from a DataSeries file.
  *
  **/
class DataSeriesSource {
public:
    /** Opens the specified file and reads its @c ExtentTypeLibary and
        its index @c Extent. Sets the current offset to the first @c
        Extent in the file.  Optionally does not read extentIndex at
        the end of the file to optimize open time when using an
        external extent index (as with dsextentindex), similarly, the check
	that the file was properly closed can also be skipped.  Note that if
	you want to read the index, the tail will be automatically checked.

        Preconditions:
            - The file must exist and must be a DataSeries file.
        Postconditions:
            - isactive() */
    DataSeriesSource(const std::string &filename, bool read_index = true, bool check_tail = true);
    ~DataSeriesSource();

    /** Returns the @c Extent at the current offset. Sets the current offset
        to the next @c Extent. If it has already reached the end of the file,
        returns null.  The @c Extent is allocated with global new. It is the
        user's responsibility to delete it.  If the source is at the end of 
	the file, returns NULL.

        Preconditions:
            - isactive() */
    Extent *readExtent() { return preadExtent(cur_offset); }

    /** Reads an Extent starting at a specified offset in the file. This
        argument will be modified to be the offset of the next Extent in the
        file.  If offset is at the end of the file, returns null. The resulting
        @c Extent is allocated using global new. It is the user's
        responsibility to delete it.

        Preconditions:
            - offset is the offset of an Extent within the file, or is
              equal to the size of the file.
            - isactive() */
    Extent *preadExtent(off64_t &offset, unsigned *compressedSize = NULL);

    /** Reads the raw Extent data from the specified offset into a
        @c ByteArray.  This byte array can be used to initialize an @c Extent.
        Updates the argument offset to be the offset of the Extent after the
        one read.

        Preconditions:
            - offset is the offset of an Extent within the file, or is equal
              to the size of the file.
            - isactive() */
    bool preadCompressed(off64_t &offset, Extent::ByteArray &bytes) {
	return Extent::preadExtent(fd,offset, bytes, need_bitflip);
    }

    /** Returns true if the file is currently open. */
    bool isactive() { return fd >= 0; }

    /** Closes the file.

        Preconditions:
            - The file must be open. */
    void closefile();

    /** Re-opens the file. Note that there is no way to change the name of
        the file.

        Preconditions:
            - The file must be closed. */
    void reopenfile();

    /** Returns a reference to an ExtentTypeLibrary that contains
        all of the types used in the file. */
    ExtentTypeLibrary &getLibrary() { return mylibrary; }

    /** This Extent describes all of the Extents and their offsets within the file. Its type
        is globally accessible through ExtentType::getDataSeriesIndexTypeV0.

        It contains the fields:
          - @c offset an int64 field which is the byte offset of the Extent within the file.
          - @c extenttype a variable32 field which is the name of the ExtentType.

        Do not modify indexExtent.
        Invariants:
          - This pointer is never null.
          - All of the extenttype fields are present in the @c ExtentTypeLibrary
            for the file. */
    Extent *indexExtent; 

    /** Returns true if the endianness of the file is different from the
        endianness of the host processor. */
    bool needBitflip() { return need_bitflip; }

    /** get the Filename associated with this file */
    const std::string &getFilename() { return filename; }
private:
    void checkHeader();
    void readTypeExtent();
    void readTailIndex();

    ExtentTypeLibrary mylibrary;

    const std::string filename;
    typedef ExtentType::byte byte;
    int fd;
    off64_t cur_offset;
    bool need_bitflip, read_index, check_tail;
};

#endif
