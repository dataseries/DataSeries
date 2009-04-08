// TODO-shirant: __BOYER_MOORE_HORSPOOL_HPP
// try to check if that is actually the convention; if it isn't, ignore
#ifndef __BOYER_MOORE_HORSPOOL_H
#define __BOYER_MOORE_HORSPOOL_H

// TODO-shirant: I don't think the ".h" is used for standard headers.
#include <inttypes.h>
#include <limits.h>
// TODO-shirant: seems unneeded, but maybe in the cpp
#include <string.h>

// TODO-shirant: I believe you based this algorithm almost directly from a text
// book. Add a comment that documents the text book you based this
// implementation on.

// TODO-shirant: although the usage is mostly clear from the interface (pass
// what you are looking for into the constructor, then call matches to see if
// you find the needle in the haystack), add doxygen comments '///' to the ctor
// and to the matches method.
class BoyerMooreHorspool {
public:
    BoyerMooreHorspool(const char *needle, int32_t needle_length);
    ~BoyerMooreHorspool();

    bool matches(const char *haystack, int32_t haystack_length);

private:
    int32_t bad_char_shift[CHAR_MAX + 1];
    char *needle;
    int32_t needle_length;
    int32_t last;
};

// TODO-shirant: coding convention is to not "close" #endif with a comment /*
// __B..._H */
#endif /* __BOYER_MOORE_HORSPOOL_H */
