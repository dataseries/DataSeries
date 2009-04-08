// TODO-shirant: should muddle with the include path so you can use <>
// if "" is widespread, ignore; could also email Eric
#include "BoyerMooreHorspool.hpp"

// TODO-shirant : will need to #include for memcpy

BoyerMooreHorspool::BoyerMooreHorspool(const char *needle, int32_t needle_length) :
        needle_length(needle_length), last(needle_length - 1) {
    // initialize the bad character shift array
    for (int32_t i = 0; i <= CHAR_MAX; ++i) {
        bad_char_shift[i] = needle_length;
    }

    for (int32_t i = 0; i < last; i++) {
        bad_char_shift[(int8_t)needle[i]] = last - i;
    }

    this->needle = new char[needle_length];
    memcpy(this->needle, needle, needle_length);
}

BoyerMooreHorspool::~BoyerMooreHorspool() {
    delete [] needle;
}

bool BoyerMooreHorspool::matches(const char *haystack, int32_t haystack_length) {
    while (haystack_length >= needle_length) {
        int32_t i;
        for (i = last; haystack[i] == needle[i]; --i) {
            if (i == 0) { // first char matches so it's a match!
                return true;
            }
        }

        int32_t skip = bad_char_shift[(int8_t)haystack[last]];
        haystack_length -= skip;
        haystack += skip;
    }
    return false;
}
