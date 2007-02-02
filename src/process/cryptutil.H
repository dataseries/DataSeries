/* -*-C++-*-
*******************************************************************************
*
* File:         cryptutil.H
* RCS:          $Header: /mount/cello/cvs/Grizzly/cpp/new-tcpdump2ds/cryptutil.H,v 1.2 2004/08/28 00:42:35 anderse Exp $
* Description:  Job, filename, etc. encryption utilities
* Author:       Eric Anderson
* Created:      Thu Dec  4 14:46:11 2003
* Modified:     Sat Jun 19 02:26:37 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __CRYPTUTIL_H
#define __CRYPTUTIL_H

#include <string>

extern bool enable_encrypt_memoize;
std::string shastring(const std::string &in);
void prepareEncrypt(const std::string &key_a, const std::string &key_b);
void prepareEncryptEnvOrRandom();
std::string encryptString(std::string in);
std::string decryptString(std::string in);

// both sqlstring and dsstring will "encrypt" to readable strings if
// the input string is on the approved list.

// dsstring encodes empty string as empty string
std::string dsstring(std::string &instr, bool do_encrypt = true);
// sqlstring encodes empty string as NULL
std::string sqlstring(std::string &instr, bool do_encrypt = true);
void printEncodeStats();

unsigned uintfield(const std::string &str);
int intfield(const std::string &str);
double dblfield(const std::string &str);
#endif
