/*
 * This file is part of PowerDNS or dnsdist.
 * Copyright -- PowerDNS.COM B.V. and its contributors
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * In addition, for the avoidance of any doubt, permission is granted to
 * link this program with OpenSSL and to (re)distribute the binaries
 * produced as the result of such linking.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
#pragma once

#include "dnsdist-lua-types.hh"
#include "iputils.hh"
#include <maxminddb.h>
#include <memory>
#include <string>

class MMDBEntryList;

class MMDB
{
public:
  MMDB(const std::string& fname, const std::string& modeStr);

  bool query(LuaAny& ret, const LuaTypeOrArrayOf<std::string>& queryParams, const ComboAddress& ip) const;
  bool exists(const ComboAddress& ip) const
  {
    MMDB_lookup_result_s res;
    return mmdbLookup(ip, res);
  }

  ~MMDB() { MMDB_close(&d_db); };

private:
  MMDB_s d_db;

  // Decodes one of the basic types (no arrays and maps)
  bool mmdbDecode(MMDB_entry_data_s* data, LuaAny& ret) const;
  // Decodes whole entry data list (supports arrays and maps too)
  bool mmdbDecodeEntryList(MMDB_entry_data_list_s** data, LuaAny& ret) const;
  bool mmdbDecodeMap(MMDB_entry_data_list_s** data, LuaAny& ret) const;
  bool mmdbDecodeArray(MMDB_entry_data_list_s** data, LuaAny& ret) const;
  bool mmdbLookup(const ComboAddress& ip, MMDB_lookup_result_s& res) const;
  std::optional<MMDBEntryList> getEntryList(MMDB_entry_s* entry) const;
};

class MMDBEntryList
{
public:
  MMDBEntryList(MMDB_entry_data_list_s* first) :
    d_entry_list_first(first, MMDB_free_entry_data_list) {}

  MMDB_entry_data_list_s* getFirst() const
  {
    return d_entry_list_first.get();
  }

private:
  std::unique_ptr<MMDB_entry_data_list_s, decltype(&MMDB_free_entry_data_list)> d_entry_list_first;
};
