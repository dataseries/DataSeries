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
    SINVARIANT(static_cast<uint32_t>(test.valInt32())== 1234567890987654321LL % 4294967296LL);
    SINVARIANT(test.valInt64() == 1234567890987654321LL);
    // TODO-brad : the previous version breaks if the compiler was
    // compiled with different floating point options than what it is
    // compiling with (e.g. fast math)
    SINVARIANT(test.valDouble() - 1234567890987654400.0 < 1000.0);
    //valString is unimplemented for all values except bool
    //SINVARIANT(test.valString() == "1234567890987654321LL");
    
    GeneralValue testAgain;
    testAgain.setDouble(1234567.890987654321);
    SINVARIANT(testAgain.valBool());
    SINVARIANT(testAgain.valInt32() == 1234567);
    SINVARIANT(testAgain.valInt64() == 1234567);
    SINVARIANT(testAgain.valDouble() == 1234567.890987654216587543487548828125);

    // TODO-brad: again, this one fails funny depending on the math lib; I'm not sure if
    // I know how to fix it.
    /*
    INVARIANT(testAgain.valByte() == static_cast<uint32_t>(1234567.890987654321) % 256,
	      boost::format("Got %d, expected %d") % ((int)testAgain.valByte())
	      % (int)(static_cast<uint8_t>(1234567.890987654216587543487548828125)) );
    */
    //valString is unimplemented for all values except bool
    //SINVARIANT(testAgain.valString() == "123456789.0987654321");
}

int main(int argc, char **argv)
{
    check_set();
    std::cout << "GeneralValue checks successful" << std::endl;
}
