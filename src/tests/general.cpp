// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/




// TODO: Test all of the other GeneralField and GeneralValue conversions

#include <DataSeries/GeneralField.hpp>
#include <boost/format.hpp>
#include <stdio.h>

void check_set()
{
    GeneralValue test;
    test.setInt64(1234567890987654321LL);
    SINVARIANT(test.valBool());
    SINVARIANT(test.valByte() == 1234567890987654321LL % 256);
    SINVARIANT(static_cast<uint32_t>(test.valInt32())== 1234567890987654321LL % 4294967296);
    SINVARIANT(test.valInt64() == 1234567890987654321LL);
    SINVARIANT(test.valDouble() == 1234567890987654400.0);
    //valString is unimplemented for all values except bool
    //SINVARIANT(test.valString() == "1234567890987654321LL");
    
    GeneralValue test2;
    test2.setDouble(1234567.890987654321);
    SINVARIANT(test2.valBool());
    SINVARIANT(test2.valByte() == static_cast<uint32_t>(1234567.890987654321) % 256);
    SINVARIANT(test2.valInt32() == 1234567);
    SINVARIANT(test2.valInt64() == 1234567);
    SINVARIANT(test2.valDouble() == 1234567.890987654216587543487548828125);
    //valString is unimplemented for all values except bool
    //SINVARIANT(test2.valString() == "123456789.0987654321");
}

int main(int argc, char **argv)
{
    check_set();
    std::cout << "GeneralValue checks successful" << std::endl;
}
