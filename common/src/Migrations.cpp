#include "gateway/common/PgUserStore.h"

// The migrations of db/migration, embedded at configure time with each file's sha256 (CMakeLists.txt writes
// generated/gateway/migrations.inc). The database records the same sha256; PgUserStore refuses any difference.

namespace openexchange { namespace gateway {

const std::vector<Migration> &embeddedMigrations()
{
    static const std::vector<Migration> all = {
#include "gateway/migrations.inc"
    };
    return all;
}

}}
