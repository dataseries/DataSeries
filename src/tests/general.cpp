// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/




// TODO: Test all of the other GeneralField and GeneralValue conversions

#include <Lintel/Double.hpp>

#include <DataSeries/GeneralField.hpp>
#include <boost/format.hpp>
#include <stdio.h>

uint8_t dblToUint8(double v);

void check_set() {
    GeneralValue test;
    test.setInt64(1234567890987654321LL);
    SINVARIANT(test.valBool());
    SINVARIANT(test.valByte() == 1234567890987654321LL % 256);
    SINVARIANT(static_cast<uint32_t>(test.valInt32())== 1234567890987654321LL % 4294967296LL);
    SINVARIANT(test.valInt64() == 1234567890987654321LL);
    SINVARIANT(Double::eq(test.valDouble(),1234567890987654400.0));
    //valString is unimplemented for all values except bool
    //SINVARIANT(test.valString() == "1234567890987654321LL");
    
    GeneralValue testAgain;
    testAgain.setDouble(1234567.890987654321);
    SINVARIANT(testAgain.valBool());
    SINVARIANT(testAgain.valInt32() == 1234567);
    SINVARIANT(testAgain.valInt64() == 1234567);
    SINVARIANT(Double::eq(testAgain.valDouble(), 1234567.890987654321));

    double v = 1234567.890987654321;
    // Re-enabling the below test.  If it fails, please document the result that you got and
    // the compiler, OS, flags that it failed under.
    INVARIANT(testAgain.valByte() == dblToUint8(v),
	      boost::format("Got %d, expected %d/%d") % static_cast<uint32_t>(testAgain.valByte())
	      % static_cast<uint32_t>(dblToUint8(v))
	      % static_cast<uint32_t>(static_cast<uint8_t>(v)));

    //valString is unimplemented for all values except bool
    //SINVARIANT(testAgain.valString() == "123456789.0987654321");
}

int main(int argc, char **argv)
{
    check_set();
    std::cout << "GeneralValue checks successful" << std::endl;
}
